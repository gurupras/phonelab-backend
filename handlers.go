package phonelab_backend

import (
	"fmt"
	"os"
)

type Work struct {
	Version         string
	DeviceId        string
	PackageName     string
	LogFileName     string
	UploadTimestamp int64
	StagingFileName string
}

var (
	PendingWorkChannel chan *Work
	DeviceWorkChannel  map[string]chan *Work
)

func DeviceWorkHandler(deviceId string, workChannel chan *Work) {
	var work *Work
	var ok bool

	for {
		if work, ok = <-workChannel; !ok {
			fmt.Fprintln(os.Stderr, fmt.Sprintf("Device-%s's work channel was closed?", deviceId))
			break
		}
		//TODO: Write logic to handle the work
		_ = work
	}
}

func PendingWorkHandler() {
	var work *Work
	var ok bool
	var deviceWorkChannel chan *Work

	PendingWorkChannel = make(chan *Work, 1000)
	DeviceWorkChannel := make(map[string]chan *Work)

	// We loop indefinitely until terminated
	for {
		//fmt.Println("Waiting for work")
		if work, ok = <-PendingWorkChannel; !ok {
			fmt.Fprintln(os.Stderr, "PendingWorkChannel closed?")
			break
		}
		//fmt.Println("Got new work")
		deviceId := work.DeviceId
		if deviceWorkChannel, ok = DeviceWorkChannel[deviceId]; !ok {
			//fmt.Println("Starting handler for device:", deviceId)
			deviceWorkChannel = make(chan *Work, 100)
			DeviceWorkChannel[deviceId] = deviceWorkChannel
			// Start the consumer for the device's work channel
			go DeviceWorkHandler(deviceId, deviceWorkChannel)
		}
		deviceWorkChannel <- work
	}
}
