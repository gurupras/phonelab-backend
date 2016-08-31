package phonelab_backend_test

import (
	"path/filepath"
	"testing"

	"github.com/gurupras/phonelab_backend"
	"github.com/stretchr/testify/assert"
)

func TestUpload(t *testing.T) {
	t.Parallel()
	var server *phonelab_backend.Server
	assert := assert.New(t)

	defer Recover("TestUpload")

	phonelab_backend.InitializeProcessingSteps()

	workFn := func(work *phonelab_backend.Work, processes ...phonelab_backend.ProcessingFunction) (err error) {
		// Dummy work function. We're only testing whether server
		// correctly receives upload and stages it
		return
	}
	config := new(phonelab_backend.Config)

	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
	go RunTestServerAsync(8083, config, &server, workFn)

	UploadFiles(8083, 3, 5, assert)
	server.Stop()
	cleanup()
}

/*
func TestLoadCapability(t *testing.T) {
	t.Parallel()
	var server *phonelab_backend.Server
	assert := assert.New(t)

	defer Recover("TestLoadCapability")

	phonelab_backend.InitializeProcessingSteps()

	config := new(phonelab_backend.Config)
	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
	go RunTestServerAsync(8084, config, &server)

	devices := LoadDevicesFromFile("deviceids.txt", assert)

	nDevices := 20

	commChannel := make(chan interface{}, 100)
	defer close(commChannel)
	channels := make([]chan interface{}, 0)

	// Function to track pending workloads
	go func() {
		pending := 0

		// Goroutine to periodically print pending uploads
		go func() {
			for {
				time.Sleep(1 * time.Second)
				logger.Debug("TLC: Pending uploads:", pending)
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
	for idx := 0; idx < nDevices; idx++ {
		device := devices[idx]

		deviceChannel := make(chan interface{})
		channels = append(channels, deviceChannel)
		wg.Add(1)
		go DeviceDataGenerator(device, 8084, commChannel, deviceChannel, wg)
	}

	time.Sleep(10 * time.Second)

	logger.Debug("Terminating ...")
	for _, channel := range channels {
		channel <- struct{}{}
	}
	wg.Wait()

	logger.Debug("Stopping server ...")
	//TODO: Server stop logic
	server.Stop()
	//cleanup()
}
*/

func TestAddStagingMetadata(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	defer Recover("TestAddStagingMetadata")

	stagingDirBase := filepath.Join(testDirBase, "staging-test-add-metadata/")
	outDirBase := filepath.Join(testDirBase, "out-test-add-metadata/")

	port := 31121
	var server *phonelab_backend.Server
	config := new(phonelab_backend.Config)
	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
	config.StagingDir = stagingDirBase
	config.OutDir = outDirBase

	phonelab_backend.InitializeStagingProcessingSteps()

	workFn := func(work *phonelab_backend.Work, processes ...phonelab_backend.ProcessingFunction) (err error) {
		// Do nothing. We're only testing adding staging metadata
		return
	}

	go RunTestServerAsync(port, config, &server, workFn)
	UploadFiles(port, 1, 1, assert)
	server.Stop()
	cleanup(stagingDirBase, outDirBase)
}
