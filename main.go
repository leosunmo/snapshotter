package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
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
	sUrl := fmt.Sprintf("http://%s:%d/RPC2", sPod.ip, sPod.port)
	fmt.Printf("Starting Supervisor client to %s...\n", sPod.name)
	sc := supervisor.New(sUrl, nil)
	_, err := sc.GetSupervisorVersion()
	if err != nil {
		return fmt.Errorf("Failed to create supervisor XMLRPC client. Error: %v", err.Error())
	}
	// wait for process to shut down
	if action == "stop" {
		fmt.Printf("Stopping %s in pod %s... ", process, sPod.name)
		success, err := sc.StopProcess(process, true)
		if err != nil {
			return fmt.Errorf("Failed to stop process %s in pod %s. Error: %v", process, sPod.name, err.Error())
		}
		if success {
			for i := 0; i < 10; i++ {
				pi, piErr := sc.GetProcessInfo(process)
				if piErr == nil && pi.State == 0 {
					fmt.Printf("Successfully stopped %s in pod %s.\n", process, sPod.name)
					return nil
				}
				time.Sleep(2 * time.Second)
			}
			return fmt.Errorf("Failed to verify that %s stopped in pod %s. Exiting.", process, sPod.name)
		} else {
			return fmt.Errorf("Failed to stop process %s in pod %s. Non success return. Success: %v", process, sPod.name, success)
		}
	}
	if action == "start" {
		fmt.Printf("Starting %s in pod %s... ", process, sPod.name)
		success, err := sc.StartProcess(process, true)
		if err != nil {
			return fmt.Errorf("Failed to start process %s in pod %s. Error: %v", process, sPod.name, err.Error())
		}
		if success {
			for i := 0; i < 5; i++ {
				pi, piErr := sc.GetProcessInfo(process)
				if piErr == nil && pi.State == 20 {
					fmt.Printf("Successfully started %s in pod %s.\n", process, sPod.name)
					return nil
				}
				time.Sleep(2 * time.Second)
			}
			fmt.Println("Failed to verify that %s started successfull in pod %s. Continuing anyway.", process, sPod.name)
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

func stopAndSnapshot(targetPods map[string]*targetPod, process string, notifyUrl *string, stopNotifyData *string, startNotifyData *string) error {
	// Iterate through the pods and stop contProcess
	defer func() {
		err := startProcessAllPods(targetPods, process)
		if err != nil {
			fmt.Println("Error: ", err.Error())
			os.Exit(1)
		}
		if *notifyUrl != "" {
			res, err := http.Post(*notifyUrl, "application/json", bytes.NewBuffer([]byte(*startNotifyData)))
			if err != nil {
				fmt.Println("Error: ", err.Error())
			}
			responseData, err := ioutil.ReadAll(res.Body)
			if err != nil {
				fmt.Println("Error: ", err.Error())
			}
			fmt.Println("Response from notify url: " + string(responseData))
		}
	}()
	if *notifyUrl != "" {
		res, err := http.Post(*notifyUrl, "application/json", bytes.NewBuffer([]byte(*stopNotifyData)))
		if err != nil {
			return err
		}
		responseData, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		fmt.Println("Response from notify url: " + string(responseData))
	}
	// Sleep to give the brokers some time to catch up and potentially change partition leadership.
	time.Sleep(10 * time.Second)
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
		fmt.Printf("Finished!\n\nSnapshot: %s\n", snapshotResult.GoString())
	}
	return nil
}

func checkAndClean(tempTargetPods map[string]*targetPod) (error, map[string]*targetPod) {
	targetPods := tempTargetPods
	check := 0
	for podName, podStruct := range targetPods {
		if podStruct.port == 0 || podStruct.volId == "" {
			check++
			delete(targetPods, podName)
		}
	}
	if check == len(targetPods) || len(targetPods) < 1 {
		return fmt.Errorf("No pods in service %s has \"xmlrpc\" named port or attached AWS EBS volume. Exiting."), targetPods
	}
	return nil, targetPods
}

func main() {
	kubeconfigPath := flag.String("kubeconfig", "./config", "path to the kubeconfig file")
	inCluster := flag.Bool("in-cluster", false, "Use in-cluster credentials")
	kubeContext := flag.String("context", "", "override current-context (default 'current-context' in kubeconfig)")
	kubeNamespace := flag.String("namespace", "", "Namespace of the pods and service")
	kubeService := flag.String("service", "", "Service to use for pod discovery")
	contProcess := flag.String("process", "", "Process to stop inside container")
	startStopNotifyUrl := flag.String("start-stop-notify-url", "", "URL to send a POST request to with (start/stop)-notify-data when starting/stopping the service")
	startNotifyData := flag.String("start-notify-data", "", "Data to send to the notify url on start of the service")
	stopNotifyData := flag.String("stop-notify-data", "", "Data to send to the notify url on stop of the service")
	flag.Parse()

	flagCheck := *contProcess
	if flagCheck == "" {
		fmt.Println("\"-process\" required.")
		flag.Usage()
		os.Exit(1)
	}

	flagCheck = *kubeService
	if flagCheck == "" {
		fmt.Println("\"-service\" required.")
		flag.Usage()
		os.Exit(1)
	}

	flagCheck = *kubeNamespace
	if flagCheck == "" {
		fmt.Println("\"-namespace\" required.")
		flag.Usage()
		os.Exit(1)
	}
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
	for _, subset := range endpoints.Subsets {
		for _, address := range subset.Addresses {
			targetPods[address.TargetRef.GetName()] = &targetPod{name: address.TargetRef.GetName(), ip: address.GetIp()}
		}
	}
	if len(targetPods) == 0 {
		fmt.Printf("Unable to find any pods using service %s. Exiting...\n", *kubeService)
		os.Exit(1)
	}
	// Get every 'targetPod' so that we can get the AWS Vol Id.
	for podName, podStruct := range targetPods {
		p, err := client.CoreV1().GetPod(ctx, podName, *kubeNamespace)
		if err != nil {
			fmt.Println("Error: ", err.Error())
			os.Exit(1)
		}
		p.Spec.Containers[0].Ports[0].GetName()
		for _, containers := range p.Spec.Containers {
			for _, port := range containers.Ports {
				if port.GetName() == "xmlrpc" {
					//podStruct = &targetPod{port: port.GetContainerPort()}
					podStruct.port = port.GetContainerPort()
					vols := p.Spec.Volumes
					for _, vol := range vols {
						awsVol := vol.VolumeSource.GetAwsElasticBlockStore()
						if awsVol == nil {
							continue
						}
						if podStruct.volId != "" {
							fmt.Println("Pod %s has more than one AWS Volume attached. Exiting...\n", p.Metadata.GetName())
							os.Exit(1)
						}
						podStruct.volId = awsVol.GetVolumeID()
						podStruct.volName = vol.GetName()
					}
				}
			}
		}
	}
	// Clean up pods and remove the ones that do not have XMLRPC or AWS VOL
	cleanErr, finalPods := checkAndClean(targetPods)
	if cleanErr != nil {
		fmt.Println(cleanErr.Error())
		os.Exit(1)
	}
	for _, pod := range finalPods {
		fmt.Println("Pod: ", pod.name, "\nPort: ", pod.port, "\nVolId: ", pod.volId)
	}
	// stop and snapshot
	err2 := stopAndSnapshot(finalPods, *contProcess, startStopNotifyUrl, stopNotifyData, startNotifyData)
	if err2 != nil {
		fmt.Println(err2.Error())
		os.Exit(1)
	}

}
