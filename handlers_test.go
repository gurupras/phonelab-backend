package phonelab_backend

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDeviceWorkHandler(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	statusChannel := make(chan string)
	channel := make(chan *Work)
	close(channel)
	go DeviceWorkHandler("dummy", channel, nil, statusChannel)
	msg, ok := <-statusChannel
	assert.True(ok, "Failed to receive status from DeviceWorkHandler")
	assert.Equal("DONE", msg, "Received invalid message from DeviceWorkHandler:", msg)

	channel = make(chan *Work)

	count := 0
	countFn := func(work *Work) {
		count++
	}

	go DeviceWorkHandler("dummy", channel, countFn, statusChannel)

	for i := 0; i < 1000; i++ {
		channel <- &Work{}
	}

	close(channel)
	msg, ok = <-statusChannel
	assert.Equal(count, 1000, "Did not process all jobs")
	assert.True(ok, "Failed to receive status from DeviceWorkHandler")
	assert.Equal("DONE", msg, "Received invalid message from DeviceWorkHandler:", msg)
}

func TestPendingWorkHandler(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	workChannel := make(chan *Work, 1000)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		PendingWorkHandler(workChannel)
	}()
	//PendingWorkChannel <- &Work{DeviceId: "dummy"}
	close(workChannel)
	wg.Wait()
	workChannel = nil

	started := 0
	verified := 0
	mutex := sync.Mutex{}
	countFn := func(work *Work) {
		mutex.Lock()
		verified++
		mutex.Unlock()
	}

	workChannel = make(chan *Work, 1000)
	// We start PendingWorkHandler in a goroutine from which we can signal back that it has returned
	pendingWorkHandlerDone := make(chan int)
	go func() {
		PendingWorkHandler(workChannel, countFn)
		close(pendingWorkHandlerDone)
	}()

	devices := LoadDevicesFromFile("./deviceids.txt", assert)

	wg = new(sync.WaitGroup)
	startedMutex := sync.Mutex{}
	workProducer := func(deviceId string, stopChannel chan interface{}) {
		//fmt.Println("workProducer:", deviceId)
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
			work := new(Work)
			work.DeviceId = deviceId
			workChannel <- work
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
	close(workChannel)
	fmt.Println("Closed workChannel")
	mutex.Unlock()

	// Wait for PendingWorkHandler to return signalling that all consumers
	// have finished consuming everything there is to consume
	_, _ = <-pendingWorkHandlerDone

	//Now confirm that all posted work was completed
	assert.Equal(started, verified, fmt.Sprintf("Started(%d) != Verified(%d)", started, verified))
}

func TestMakeStagedFilesPending(t *testing.T) {
	t.Parallel()
	var err error
	assert := assert.New(t)

	// First, finish the negative cases
	err = MakeStagedFilesPending("/deadbeef", nil)
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
		numIterations int = 3
		nDevices      int = 5
		startPort     int = 9200
	)
	for i := 0; i < numIterations; i++ {
		var nFilesPerDevice int = 3 + rand.Intn(3) // [3,5]
		var server *Server
		port := startPort + i
		workChannel := make(chan *Work, 1000)
		go RunTestServerAsync(port, workChannel, &server)

		UploadFiles(port, nDevices, nFilesPerDevice, assert)
		totalFiles := nDevices * nFilesPerDevice

		// Now close server
		fmt.Println("Waiting for server to stop")
		server.Stop()
		fmt.Println("Server stopped")

		// By now all the device handlers should have staged the files
		workChannel = make(chan *Work, 1000)
		mutex := sync.Mutex{}
		verified := 0
		countFn := func(work *Work) {
			mutex.Lock()
			verified++
			if verified == totalFiles {
				close(workChannel)
			}
			assert.False(verified > totalFiles, "Verified more than total files?")
			mutex.Unlock()
		}
		wg := new(sync.WaitGroup)
		wg.Add(1)
		go func() {
			defer wg.Done()
			PendingWorkHandler(workChannel, countFn)
		}()
		wg.Wait()
		cleanup()
	}
}
