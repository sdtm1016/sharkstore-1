#!/bin/bash
BUILD_VERSION=`git describe --tags --dirty --always`
BUILD_DATE=`date +"%Y/%m/%d-%H:%M:%S"`

go build -ldflags "-X main.BuildVersion=${BUILD_VERSION} -X main.BuildDate=${BUILD_DATE}" -o master-server
