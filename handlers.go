package phonelab_backend

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"gopkg.in/yaml.v2"

	"github.com/gurupras/gocommons"
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

func MakeStagedFilesPending(stagingDir string) error {
	var err error
	files, err := gocommons.ListFiles(stagingDir, []string{"log-*"})
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}

	stagedFileToPendingWork := func(filePath string) {
		defer wg.Done()
		var file *os.File

		file, err = os.OpenFile(filePath, os.O_RDONLY, 0)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to open staged file to move to pending work\n", err)
			return
		}
		// XXX: Hard-coded to 1K
		buf := new(bytes.Buffer)
		_, err = io.CopyN(buf, file, 1024)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to read data from staged file\n", err)
			return
		}
		stagingMetadata := StagingMetadata{}
		err = yaml.Unmarshal(buf.Bytes(), &stagingMetadata)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to unmarshall staging metadata\n", err)
			return
		}
		work := &Work{
			StagingFileName: filePath,
			Version:         stagingMetadata.Version,
			DeviceId:        stagingMetadata.DeviceId,
			PackageName:     stagingMetadata.PackageName,
			UploadTimestamp: stagingMetadata.UploadTimestamp,
		}
		PendingWorkChannel <- work
	}
	for _, file := range files {
		// Read the YAML metadata and create a work struct from it
		wg.Add(1)
		go stagedFileToPendingWork(file)
	}
	wg.Wait()
	return err
}

func ProcessStagedWork(work *Work) {
}

func DeviceWorkHandler(deviceId string, workChannel chan *Work, workFn func(work *Work), statusChannel chan string) {
	var work *Work
	var ok bool

	for {
		if work, ok = <-workChannel; !ok {
			if statusChannel != nil {
				statusChannel <- "DONE"
			}
			break
		}
		//TODO: Write logic to handle the work
		// We need to pick up files from the staging area, process them, and move the result to 'out'.
		// We could do per-work goroutines here, but for now, we keep it simple.
		workFn(work)
	}
}

func PendingWorkHandler(workFuncs ...func(work *Work)) {
	var work *Work
	var ok bool
	var deviceWorkChannel chan *Work

	// TODO: Currently, we only use the first function passed in
	var workFunc func(work *Work)
	if workFuncs != nil && len(workFuncs) > 0 {
		workFunc = workFuncs[0]
	} else {
		workFunc = ProcessStagedWork
	}

	PendingWorkChannel = make(chan *Work, 1000)
	DeviceWorkChannel := make(map[string]chan *Work)

	// Find all files in the staging area and re-assign them as pending work
	go MakeStagedFilesPending(StagingDirBase)

	wg := sync.WaitGroup{}
	// Local function wrapping around DeviceWorkHandler to ensure that
	// we wait for all device handlers to terminate before we return
	dwh := func(deviceId string, deviceWorkChannel chan *Work) {
		defer wg.Done()
		DeviceWorkHandler(deviceId, deviceWorkChannel, workFunc, nil)
	}

	// We loop indefinitely until terminated
	delegated := 0
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
			deviceWorkChannel = make(chan *Work, 100000)
			DeviceWorkChannel[deviceId] = deviceWorkChannel
			// Start the consumer for the device's work channel
			wg.Add(1)
			go dwh(deviceId, deviceWorkChannel)
		}
		deviceWorkChannel <- work
		delegated++
	}

	for device := range DeviceWorkChannel {
		close(DeviceWorkChannel[device])
	}
	wg.Wait()
	fmt.Println(fmt.Sprintf("Delegated %d tasks", delegated))
}
