package phonelab_backend_test

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gurupras/phonelab_backend"
	"github.com/stretchr/testify/assert"
)

func TestDeviceWorkHandler(t *testing.T) {
	//t.Parallel()
	assert := assert.New(t)

	defer Recover("TestDeviceWorkHandler", assert)

	statusChannel := make(chan string)
	channel := make(chan *phonelab_backend.Work)
	close(channel)
	go phonelab_backend.DeviceWorkHandler("dummy", channel, nil, statusChannel)
	msg, ok := <-statusChannel
	assert.True(ok, "Failed to receive status from DeviceWorkHandler")
	assert.Equal("DONE", msg, "Received invalid message from DeviceWorkHandler:", msg)

	channel = make(chan *phonelab_backend.Work)

	count := 0
	countMutex := sync.Mutex{}

	countFn := func(work *phonelab_backend.DeviceWork, processingConfig *phonelab_backend.ProcessingConfig) (err error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		count++
		return
	}

	processingConfig := new(phonelab_backend.ProcessingConfig)
	processingConfig.Core = countFn

	go phonelab_backend.DeviceWorkHandler("dummy", channel, processingConfig, statusChannel)

	for i := 0; i < 1000; i++ {
		channel <- &phonelab_backend.Work{}
	}

	close(channel)
	msg, ok = <-statusChannel
	assert.Equal(count, 1000, "Did not process all jobs")
	assert.True(ok, "Failed to receive status from DeviceWorkHandler")
	assert.Equal("DONE", msg, "Received invalid message from DeviceWorkHandler:", msg)
}

func TestPendingWorkHandler(t *testing.T) {
	//t.Parallel()
	var err error
	assert := assert.New(t)

	defer Recover("TestPendingWorkHandler", assert)

	// XXX: What is this first part testing?
	config := new(phonelab_backend.Config)
	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
	config.StagingDir, err = ioutil.TempDir(testDirBase, "staging-")
	assert.Nil(err, "Failed to create staging dir")

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		phonelab_backend.PendingWorkHandler(config)
	}()
	//phonelab_backend.PendingWorkChannel <- &phonelab_backend.Work{DeviceId: "dummy"}
	close(config.WorkChannel)
	wg.Wait()
	config.WorkChannel = nil

	// Now we do some _real_ work
	started := 0
	verified := 0
	mutex := sync.Mutex{}
	countFn := func(work *phonelab_backend.DeviceWork, processingConfig *phonelab_backend.ProcessingConfig) (err error) {
		mutex.Lock()
		verified++
		mutex.Unlock()
		return
	}

	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
	config.ProcessingConfig = new(phonelab_backend.ProcessingConfig)
	config.ProcessingConfig.Core = countFn
	// We start PendingWorkHandler in a goroutine from which we can signal back that it has returned
	pendingWorkHandlerDone := make(chan int)
	go func() {
		defer close(pendingWorkHandlerDone)
		phonelab_backend.PendingWorkHandler(config)
	}()

	devices := LoadDevicesFromFile("./deviceids.txt", assert)

	wg = new(sync.WaitGroup)
	startedMutex := sync.Mutex{}
	workProducer := func(deviceId string, stopChannel chan interface{}) {
		logger.Debug("workProducer:", deviceId)
		defer wg.Done()
		stop := false

		go func() {
			_ = <-stopChannel
			startedMutex.Lock()
			stop = true
			startedMutex.Unlock()
		}()

		for {
			startedMutex.Lock()
			if stop {
				startedMutex.Unlock()
				break
			}
			work := new(phonelab_backend.Work)
			work.DeviceId = deviceId
			config.WorkChannel <- work
			started++
			startedMutex.Unlock()
		}
	}

	stopChannel := make(chan interface{})

	producers := 20

	for idx := 0; idx < producers; idx++ {
		wg.Add(1)
		go workProducer(devices[idx], stopChannel)
	}

	time.Sleep(1 * time.Second)

	// Stop all producers
	for idx := 0; idx < producers; idx++ {
		stopChannel <- struct{}{}
	}
	// Wait for producers to terminate
	wg.Wait()

	// All producers have terminated. The number of tasks posted into
	// PendingWorkChannel cannot change anymore.
	// Close the PendingWorkChannel to trigger stopping of consumers
	mutex.Lock()
	close(config.WorkChannel)
	logger.Debug("Closed workChannel")
	mutex.Unlock()

	// Wait for PendingWorkHandler to return signalling that all consumers
	// have finished consuming everything there is to consume
	_, _ = <-pendingWorkHandlerDone

	//Now confirm that all posted work was completed
	assert.Equal(started, verified, fmt.Sprintf("Started(%d) != Verified(%d)", started, verified))
}

func TestMakeStagedFilesPending(t *testing.T) {
	//t.Parallel()
	var err error
	assert := assert.New(t)

	defer Recover("TestMakeStagedFilesPending", assert)

	stagingDirBase := filepath.Join(testDirBase, "staging-makestagedfilespending")
	outDirBase := filepath.Join(testDirBase, "out-makestagedfilespending")

	// First, finish the negative cases
	config := new(phonelab_backend.Config)
	config.StagingDir = "/deadbeef"
	err = phonelab_backend.MakeStagedFilesPending(config)
	assert.NotNil(err, "No error on non-existing stagingDir")

	// We first generate some logs, close PendingWork* and then
	// restart it to have it run MakeStagedFilesPending.
	// By adding a custom workFn, we can count the number of pending tasks
	// and ensure that it matches the number of files that was created
	// from the generation step

	// Unfortunately, to generate the logs, and have them moved to staging,
	// we need to run the server. So go ahead and do that

	// Run the test a few times to make sure it works every time
	var (
		numIterations int = 10
		nDevices      int = 5
		startPort     int = 9200
	)

	var configs []*phonelab_backend.Config
	for i := 0; i < numIterations; i++ {
		config := new(phonelab_backend.Config)
		configs = append(configs, config)
		config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
		config.StagingDir = fmt.Sprintf("%s-%d", stagingDirBase, i)
		config.OutDir = fmt.Sprintf("%s-%d", outDirBase, i)
		config.ProcessingConfig = new(phonelab_backend.ProcessingConfig)
	}

	iterWg := sync.WaitGroup{}

	iterFn := func(idx int) {
		defer iterWg.Done()

		defer Recover(fmt.Sprintf("TestMakeStagedFilesPending-%d", idx), assert)

		var nFilesPerDevice int = 3 + rand.Intn(3) // [3,5]
		var server *phonelab_backend.Server

		config := configs[idx]
		port := startPort + idx

		totalFiles := 0

		workFn := func(work *phonelab_backend.DeviceWork, processingConfig *phonelab_backend.ProcessingConfig) (err error) {
			totalFiles++
			return
		}
		config.ProcessingConfig.Core = workFn

		serverWg := sync.WaitGroup{}
		serverWg.Add(1)
		go func() {
			defer serverWg.Done()
			RunTestServerAsync(port, config, &server)
		}()
		UploadFiles(port, nDevices, nFilesPerDevice, assert)

		// Now close server
		logger.Debug("Waiting for server to stop")
		server.Stop()
		serverWg.Wait()
		logger.Debug("Server stopped")
		config.WorkChannel = nil
		logger.Debug("Set work channel to nil")

		// By now all the device handlers should have staged the files
		config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
		config.ProcessingConfig = new(phonelab_backend.ProcessingConfig)
		mutex := sync.Mutex{}
		wg := new(sync.WaitGroup)

		var verified int = 0
		countFn := func(work *phonelab_backend.DeviceWork, processingconfig *phonelab_backend.ProcessingConfig) (err error) {
			mutex.Lock()
			verified++
			logger.Debug(fmt.Sprintf("%d - %d/%d", idx, verified, totalFiles))
			if verified == totalFiles {
				config.CloseWorkChannel()
			}
			mutex.Unlock()
			return
		}
		config.ProcessingConfig.Core = countFn

		wg.Add(1)
		go func() {
			defer wg.Done()
			phonelab_backend.PendingWorkHandler(config)
		}()
		wg.Wait()
		assert.Equal(totalFiles, verified, fmt.Sprintf("total(%d) != verified(%d)", totalFiles, verified))
		cleanup(config.StagingDir, config.OutDir)
	}

	for i := 0; i < numIterations; i++ {
		iterWg.Add(1)
		go iterFn(i)
	}
	iterWg.Wait()
}
