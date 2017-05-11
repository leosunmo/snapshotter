#!/bin/bash

if [ -z ${1+x} ]; then echo "Usage: ./build.sh \$tag"; exit; fi

CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o snapshotter ../.

sudo docker build -t  snapshotter:$1 .

