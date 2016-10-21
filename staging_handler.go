package phonelab_backend

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fatih/set"
	"github.com/gurupras/gocommons"
)

type StagingProcess func(work *Work) (error, bool)

type StagingConfig struct {
	PreProcessing  []StagingProcess
	PostProcessing []StagingProcess
}

type StagingMetadata struct {
	UploadMetadata
	Dates []time.Time `yaml:dates`
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

func InitializeStagingConfig() *StagingConfig {
	sc := new(StagingConfig)

	sc.PostProcessing = append(sc.PostProcessing, MakeStagedFileReadOnly)
	return sc
}

func MakeStagedFilesPending(config *Config) error {
	var err error
	files, err := gocommons.ListFiles(config.StagingDir, []string{"log-*"})
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}

	stagedFileToPendingWork := func(filePath string) {
		defer wg.Done()
		var stagingMetadata *StagingMetadata
		if stagingMetadata, err = parseStagingMetadataFromFile(filePath); err != nil {
			fmt.Fprintln(os.Stderr, fmt.Sprintf("Failed to parse staging metadata from file '%v': %v", filePath, err))
			return
		}
		work := &Work{
			StagingMetadata: *stagingMetadata,
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
			//logger.Debugln(fmt.Sprintf("%s -> Merging %d files to out", deviceId, len(workList)))
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

	logger.Debugln(fmt.Sprintf("%s -> workChannel closed..waiting for all pending work to complete", deviceId))

	// Wait for all work to complete
	for {
		size := workSet.Size()
		if size > 0 {
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
	// Signal workset processor to stop
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
	logger.Debugln("config.WorkChannel closed..stopping all device work channels")

	for device := range DeviceWorkChannel {
		close(DeviceWorkChannel[device])
	}
	logger.Debugln("Stopped all device work channels..waiting for all device work handlers to return")
	wg.Wait()
	logger.Debugln(fmt.Sprintf("PendingWorkHandler() finished!  Delegated %d tasks", delegated))
}
