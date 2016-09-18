package phonelab_backend

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/fatih/set"
	"github.com/gurupras/gocommons"
	"github.com/gurupras/gocommons/seekable_stream"
)

type UploadMetadata struct {
	Version         string `yaml:version`
	DeviceId        string `yaml:device_id`
	PackageName     string `yaml:package_name`
	UploadTimestamp int64  `yaml:upload_timestamp`
	UploadFileName  string
}

type StagingMetadata struct {
	UploadMetadata
	Dates []time.Time `yaml:dates`
}

type Work struct {
	UploadMetadata
	StagingMetadata StagingMetadata
	StagingFileName string
	StagingDir      string
	OutDir          string
	DataStream      *seekable_stream.SeekableStream
}

type Config struct {
	WorkChannel     chan *Work
	StagingDir      string
	OutDir          string
	WorkChannelLock sync.Mutex
	*StagingConfig
	*ProcessingConfig
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
			StagingMetadata: stagingMetadata,
			StagingFileName: filePath,
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
	//fmt.Println(fmt.Sprintf("Staged %s - %d", config.StagingDir, len(files)))
	return err
}

func DeviceWorkHandler(deviceId string, workChannel chan *Work, processingConfig *ProcessingConfig, statusChannel chan string) (err error) {
	var stop int32 = 0

	if statusChannel != nil {
		defer close(statusChannel)
	}

	workSet := set.New()

	if processingConfig == nil {
		err = errors.New(fmt.Sprintf("Incomplete processingConfig..please make sure all fields are initialized"))
		return
	}
	// Goroutine to go through the set and issue work
	processWorkSetWg := sync.WaitGroup{}
	processWorkSetWg.Add(1)
	go func() {
		defer processWorkSetWg.Done()

		processWg := sync.WaitGroup{}

		processRoutine := func(workList []*Work, processingConfig *ProcessingConfig) {
			defer processWg.Done()
			logger.Debugln(fmt.Sprintf("%s -> Merging %d files to out", deviceId, len(workList)))
			if err = ProcessProcessConfig(workList, processingConfig); err != nil {
				// TODO: What should we do here?
				// We're in a goroutine, and so, should we
				// return error via a channel?

				// For now, just panic
				panic(err.Error())
			}
		}

		for {
			if stop != 0 {
				break
			}
			now := time.Now()

			datesToProcess := set.NewNonTS()
			dateWorkMap := make(map[time.Time][]*Work)

			for _, obj := range workSet.List() {
				work := obj.(*Work)

				if len(work.StagingMetadata.Dates) > 1 {
					fmt.Fprintln(os.Stderr, fmt.Sprintf("Warning: More than 1 date: %s -> %v", work.StagingFileName, work.StagingMetadata.Dates))
				}

				for _, date := range work.StagingMetadata.Dates {
					if now.Sub(date) > processingConfig.DelayBeforeProcessing {
						if !datesToProcess.Has(date) {
							datesToProcess.Add(date)
							dateWorkMap[date] = make([]*Work, 0)
						}
						dateWorkMap[date] = append(dateWorkMap[date], work)
						workSet.Remove(work)
					}
				}

			}
			//logger.Debugln(fmt.Sprintf("%s -> Remaining work (%d)", deviceId, len(workSet.List())))

			for _, obj := range datesToProcess.List() {
				date := obj.(time.Time)

				processWg.Add(1)
				go processRoutine(dateWorkMap[date], processingConfig)

			}
			time.Sleep(processingConfig.WorkSetCheckPeriod)
		}
		//fmt.Println("Waiting for running process handlers to complete...")
		processWg.Wait()
	}()

	var work *Work
	var ok bool
	received := int64(0)
	for {
		if work, ok = <-workChannel; !ok {
			break
		}

		received++
		//logger.Debugln(fmt.Sprintf("%s -> #Work: %d", deviceId, received))

		// Just keep appending to an array of work and let the
		// internal goroutine schedule these when applicable
		workSet.Add(work)
		//logger.Debugln(fmt.Sprintf("%s -> workSet.size(): %d", deviceId, workSet.Size()))
	}
	//XXX: Wait for all work to complete
	for {
		size := workSet.Size()
		if size > 0 {
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
	atomic.AddInt32(&stop, 1)
	processWorkSetWg.Wait()
	if statusChannel != nil {
		statusChannel <- "DONE"
	}
	//fmt.Println("DeviceWorkHandler - finished")
	return
}

func PendingWorkHandler(config *Config) {
	var work *Work
	var ok bool
	var deviceWorkChannel chan *Work

	DeviceWorkChannel := make(map[string]chan *Work)

	// Find all files in the staging area and re-assign them as pending work
	config.WorkChannelLock.Lock()
	MakeStagedFilesPending(config)
	config.WorkChannelLock.Unlock()

	wg := sync.WaitGroup{}
	// Local function wrapping around DeviceWorkHandler to ensure that
	// we wait for all device handlers to terminate before we return
	dwh := func(deviceId string, deviceWorkChannel chan *Work) (err error) {
		defer wg.Done()
		err = DeviceWorkHandler(deviceId, deviceWorkChannel, config.ProcessingConfig, nil)
		return err
	}

	// We loop indefinitely until terminated
	delegated := 0
	for {
		//fmt.Println("Waiting for work")
		if work, ok = <-config.WorkChannel; !ok {
			//fmt.Fprintln(os.Stderr, "workChannel closed?")
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
