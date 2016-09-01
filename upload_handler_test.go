package phonelab_backend_test

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gurupras/phonelab_backend"
	"github.com/stretchr/testify/assert"
)

func TestStaging(t *testing.T) {
	t.Parallel()

	var port int = 8083
	var server *phonelab_backend.Server
	assert := assert.New(t)

	defer Recover("TestStaging")

	dummyWork := func(work *phonelab_backend.DeviceWork, processingConfig *phonelab_backend.ProcessingConfig) (err error) {
		// Dummy work function. We're only testing whether server
		// correctly receives upload and stages it
		return
	}

	config := new(phonelab_backend.Config)

	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)

	config.StagingConfig = phonelab_backend.InitializeStagingConfig()
	config.ProcessingConfig = new(phonelab_backend.ProcessingConfig)
	config.ProcessingConfig.Core = dummyWork

	go RunTestServerAsync(port, config, &server)

	UploadFiles(port, 3, 5, assert)
	server.Stop()
	cleanup()
}

func TestUpload(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	var port int = 8084
	var server *phonelab_backend.Server

	defer Recover("TestUpload")

	config := new(phonelab_backend.Config)

	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
	config.ProcessingConfig = phonelab_backend.InitializeProcessingConfig()

	count := 0
	countFn := func(work *phonelab_backend.DeviceWork) (err error, fail bool) {
		count++
		return
	}
	config.ProcessingConfig.PostProcessing = append(config.ProcessingConfig.PostProcessing, countFn)

	go RunTestServerAsync(port, config, &server)

	UploadFiles(port, 3, 5, assert)
	server.Stop()
	assert.Equal(15, count, "Did not process expected # of uploaded files")
	cleanup()
}

func TestLoadCapability(t *testing.T) {
	t.Parallel()

	t.Skip("TestLoadCapability: Skipping until logic for evaluating output is decided")

	assert := assert.New(t)

	var port int = 8085
	var server *phonelab_backend.Server

	defer Recover("TestLoadCapability")

	config := new(phonelab_backend.Config)
	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
	go RunTestServerAsync(port, config, &server)

	devices := LoadDevicesFromFile("deviceids.txt", assert)

	nDevices := 30

	commChannel := make(chan interface{}, 100)

	// Function to track pending workloads
	go func() {
		pending := 0

		// Goroutine to periodically print pending uploads
		go func() {
			for {
				time.Sleep(1 * time.Second)
				logger.Info("TLC: Pending uploads:", pending)
			}
		}()

		for {
			if object, ok := <-commChannel; !ok {
				break
			} else {
				state := object.(int)
				switch state {
				case PENDING:
					pending++
				case DONE:
					pending--
				default:
					logger.Error("What is this? %v", state)
				}
			}
		}
	}()

	wg := new(sync.WaitGroup)

	start := time.Now().UnixNano()
	testTimeNanos := int64(30 * 1e9)

	dataGenerator := func(deviceId string) {
		dataRequestChannel := make(chan interface{})
		go func() {
			for {
				now := time.Now().UnixNano()
				if now-start > testTimeNanos {
					break
				}
				// Generate requests as fast as possible
				// Therefore, no sleeps
				dataRequestChannel <- struct{}{}
			}
			close(dataRequestChannel)
		}()
		DeviceDataGenerator(deviceId, port, commChannel, dataRequestChannel, wg)
	}

	for idx := 0; idx < nDevices; idx++ {
		device := devices[idx]
		wg.Add(1)
		go dataGenerator(device)
	}

	logger.Debug("Waiting to terminate ...")
	wg.Wait()
	logger.Debug("Terminating ...")

	logger.Debug("Stopping server ...")
	//TODO: Server stop logic
	server.Stop()
	//cleanup()
}

func TestAddStagingMetadata(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	defer Recover("TestAddStagingMetadata")

	stagingDirBase := filepath.Join(testDirBase, "staging-test-add-metadata/")
	outDirBase := filepath.Join(testDirBase, "out-test-add-metadata/")

	port := 8086
	var server *phonelab_backend.Server
	config := new(phonelab_backend.Config)
	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
	config.StagingDir = stagingDirBase
	config.OutDir = outDirBase

	config.ProcessingConfig = new(phonelab_backend.ProcessingConfig)
	dummyWork := func(work *phonelab_backend.DeviceWork, processingConfig *phonelab_backend.ProcessingConfig) (err error) {
		// Do nothing. We're only testing adding staging metadata
		return
	}
	config.ProcessingConfig.Core = dummyWork

	go RunTestServerAsync(port, config, &server)
	UploadFiles(port, 1, 1, assert)
	server.Stop()
	cleanup(stagingDirBase, outDirBase)
}
