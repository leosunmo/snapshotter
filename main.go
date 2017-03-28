package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/ericchiang/k8s"
	"github.com/ghodss/yaml"
	supervisor "github.com/ligustah/go-supervisor"
)

var (
	ctx = context.Background()
)

type targetPod struct {
	name    string
	volId   string
	volName string
	ip      string
	port    int32
}

func loadClient(kubeconfigPath, kubeContext string, inCluster bool) (*k8s.Client, error) {
	if inCluster {
		client, err := k8s.NewInClusterClient()
		if err != nil {
			return nil, fmt.Errorf("Failed to create in-cluster client: %v", err)
		}
		return client, nil
	}
	data, err := ioutil.ReadFile(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("Failed to read kubeconfig: %v", err)
	}

	// Unmarshal YAML into a Kubernetes config object.
	var config k8s.Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("Failed to unmarshal kubeconfig: %v", err)
	}
	if kubeContext != "" {
		config.CurrentContext = kubeContext
	}
	return k8s.NewClient(&config)
}

func stopProcessAllPods(targetPods map[string]*targetPod, process string) error {
	var errors []string
	for _, sPod := range targetPods {
		err := stopProcess(sPod, process)
		if err != nil {
			errors = append(errors, err.Error())
		}
	}
	if len(errors) > 0 {
		return fmt.Errorf(strings.Join(errors, "\n"))
	}
	return nil
}

func startProcessAllPods(targetPods map[string]*targetPod, process string) error {
	var errors []string
	for _, sPod := range targetPods {
		err := startProcess(sPod, process)
		if err != nil {
			errors = append(errors, err.Error())
		}
	}
	if len(errors) > 0 {
		return fmt.Errorf(strings.Join(errors, "\n"))
	}
	return nil
}

func stopProcess(sPod *targetPod, process string) error {
	return stopStartProcess(sPod, process, "stop")
}

func startProcess(sPod *targetPod, process string) error {
	return stopStartProcess(sPod, process, "start")
}

func stopStartProcess(sPod *targetPod, process, action string) error {
	sUrl := fmt.Sprintf("http://%s:%s/RPC2", sPod.ip, sPod.port)
	sc := supervisor.New(sUrl, nil)
	_, err := sc.GetSupervisorVersion()
	if err != nil {
		return fmt.Errorf("Failed to create supervisor XMLRPC client. Error: %v", err.Error())
	}
	// wait for process to shut down
	if action == "stop" {
		success, err := sc.StopProcess(process, true)
		if err != nil {
			return fmt.Errorf("Failed to stop process %s in pod %s. Error: %v", process, sPod.name, err.Error())
		}
		if success {
			return nil
		} else {
			return fmt.Errorf("Failed to stop process %s in pod %s. Non success return. Success: %v", process, sPod.name, success)
		}
	}
	if action == "start" {
		success, err := sc.StartProcess(process, true)
		if err != nil {
			return fmt.Errorf("Failed to start process %s in pod %s. Error: %v", process, sPod.name, err.Error())
		}
		if success {
			return nil
		} else {
			return fmt.Errorf("Failed to start process %s in pod %s. Non success return. Success: %v", process, sPod.name, success)
		}
	}
	return fmt.Errorf("Invalid action command")
}

func newEC2Client(region string) (ec2Client *ec2.EC2) {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
	ec2Client = ec2.New(sess)
	return
}

func newSnapshotConfig(svc, volId string) ec2.CreateSnapshotInput {
	date := time.Now().Format("2006-01-02-15-04")
	snapshotConfig := ec2.CreateSnapshotInput{
		VolumeId:    aws.String(volId),
		Description: aws.String(fmt.Sprintf("[%s] Snapshot of %s", date, svc)),
		DryRun:      aws.Bool(false),
	}
	return snapshotConfig
}

func stopAndSnapshot(targetPods map[string]*targetPod, process string) error {
	// Iterate through the pods and stop Kafka
	defer startProcessAllPods(targetPods, process)
	err := stopProcessAllPods(targetPods, process)
	if err != nil {
		return err
	}

	// new AWS client to start snapshot jobs on all the awsvols from the pods.
	ec2Client := newEC2Client("us-east-1")
	for _, sPod := range targetPods {
		snapshotConfig := newSnapshotConfig(sPod.name, sPod.volId)
		fmt.Printf("Starting snapshot for pod: %s...", sPod.name)
		snapshotResult, err := ec2Client.CreateSnapshot(&snapshotConfig)
		if err != nil {
			return fmt.Errorf("Snapshot error: %v", err.Error())
		}
		fmt.Printf("Finished!\n\nSnapshot: %s", snapshotResult.GoString())
	}
	return nil
}

func main() {
	kubeconfigPath := flag.String("kubeconfig", "./config", "path to the kubeconfig file")
	inCluster := flag.Bool("in-cluster", false, "Use in-cluster credentials")
	kubeContext := flag.String("context", "", "override current-context (default 'current-context' in kubeconfig)")
	kubeNamespace := flag.String("namespace", "", "specific namespace (default all namespaces)")
	kubeService := flag.String("service", "", "Service to use for pod discovery")
	flag.Parse()

	//uses the current context in kubeconfig unless overriden using '-context'
	client, err := loadClient(*kubeconfigPath, *kubeContext, *inCluster)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	endpoints, err := client.CoreV1().GetEndpoints(ctx, *kubeService, *kubeNamespace)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	targetPods := make(map[string]*targetPod)
	// Find all pods that are selected by the service
	// Need to loop over subset.Ports to search subset.Ports[*].Name for xmlrpc and grab it
	for _, subset := range endpoints.Subsets {
		for _, port := range subset.Ports {
			if port.GetName() == "xmlrpc" {
				for _, address := range subset.Addresses {
					targetPods[address.TargetRef.GetName()] = &targetPod{name: address.TargetRef.GetName(), ip: address.GetIp(), port: port.GetPort()}
				}
			}
		}
	}
	if len(targetPods) == 0 {
		fmt.Printf("Unable to find any pods with a port names \"xmlrpc\" using service %s. Exiting...\n", *kubeService)
		os.Exit(1)
	}
	// Get every 'targetPod' so that we can get the AWS Vol Id.
	for podName, _ := range targetPods {
		p, err := client.CoreV1().GetPod(ctx, podName, *kubeNamespace)
		if err != nil {
			fmt.Println("Error: ", err.Error())
			os.Exit(1)
		}
		vols := p.Spec.Volumes
		for _, vol := range vols {
			awsVol := vol.VolumeSource.GetAwsElasticBlockStore()
			if awsVol == nil {
				continue
			}
			if targetPods[p.Metadata.GetName()].volId != "" {
				fmt.Println("Pod %s has more than one AWS Volume attached. Exiting...\n", p.Metadata.GetName())
				os.Exit(1)
			}
			targetPods[p.Metadata.GetName()].volId = awsVol.GetVolumeID()
			targetPods[p.Metadata.GetName()].volName = vol.GetName()
		}

	}
	// stop and snapshot
	err2 := stopAndSnapshot(targetPods, "kafka")
	if err2 != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

}
