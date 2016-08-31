package phonelab_backend_test

import (
	"path/filepath"
	"testing"

	"github.com/gurupras/phonelab_backend"
	"github.com/stretchr/testify/assert"
)

/*
func TestUpload(t *testing.T) {
	t.Parallel()
	var server *phonelab_backend.Server
	assert := assert.New(t)

	defer Recover("TestUpload")

	phonelab_backend.InitializeProcessingSteps()

	workFn := func(work *phonelab_backend.Work) {
		// Dummy work function. We're only testing whether server
		// correctly receives upload and stages it
	}
	workChannel := make(chan *phonelab_backend.Work, 1000)
	go RunTestServerAsync(8083, "", "", workChannel, &server, workFn)

	UploadFiles(8083, 1, 1, assert)
	server.Stop()
	cleanup()
}
*/
/*
func TestLoadCapability(t *testing.T) {
	t.Parallel()
	var server *phonelab_backend.Server
	assert := assert.New(t)

	defer Recover("TestLoadCapability")

	phonelab_backend.InitializeProcessingSteps()

	workChannel := make(chan *phonelab_backend.Work, 1000)
	go RunTestServerAsync(8084, "", "", workChannel, &server)

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
				fmt.Println("TLC: Pending uploads:", pending)
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
					fmt.Fprintln(os.Stderr, "What is this? %v", state)
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

	fmt.Println("Terminating ...")
	for _, channel := range channels {
		channel <- struct{}{}
	}
	wg.Wait()

	fmt.Println("Stopping server ...")
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

	workFn := func(work *phonelab_backend.Work) {
		// Do nothing. We're only testing adding staging metadata
	}

	go RunTestServerAsync(port, config, &server, workFn)
	UploadFiles(port, 1, 1, assert)
	server.Stop()
	cleanup(stagingDirBase, outDirBase)
}
