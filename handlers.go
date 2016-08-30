package phonelab_backend

import (
	"bytes"
	"compress/gzip"
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
	StagingDir      string
	OutDir          string
}

type Config struct {
	WorkChannel     chan *Work
	StagingDir      string
	OutDir          string
	WorkChannelLock sync.Mutex
}

func (c *Config) CloseWorkChannel() {
	c.WorkChannelLock.Lock()
	close(c.WorkChannel)
	c.WorkChannelLock.Unlock()
}

var (
	DeviceWorkChannel map[string]chan *Work
)

func MakeStagedFilesPending(config *Config) error {
	var err error
	files, err := gocommons.ListFiles(config.StagingDir, []string{"log-*"})
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

		var gzipReader *gzip.Reader
		if gzipReader, err = gzip.NewReader(buf); err != nil {
			fmt.Fprintln(os.Stderr, "Failed to obtain reader to compressed stream", err)
			return
		}
		uncompressedBuf := new(bytes.Buffer)
		io.Copy(uncompressedBuf, gzipReader)

		stagingMetadata := StagingMetadata{}
		err = yaml.Unmarshal(uncompressedBuf.Bytes(), &stagingMetadata)
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
			OutDir:          config.OutDir,
		}
		config.WorkChannel <- work
	}
	for _, file := range files {
		// Read the YAML metadata and create a work struct from it
		wg.Add(1)
		go stagedFileToPendingWork(file)
	}
	wg.Wait()
	fmt.Println(fmt.Sprintf("Staged %s - %d", config.StagingDir, len(files)))
	return err
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

func PendingWorkHandler(config *Config, workFuncs ...func(work *Work)) {
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

	DeviceWorkChannel := make(map[string]chan *Work)

	// Find all files in the staging area and re-assign them as pending work
	config.WorkChannelLock.Lock()
	MakeStagedFilesPending(config)
	config.WorkChannelLock.Unlock()

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
		if work, ok = <-config.WorkChannel; !ok {
			fmt.Fprintln(os.Stderr, "workChannel closed?")
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
	//fmt.Println(fmt.Sprintf("Delegated %d tasks", delegated))
}
