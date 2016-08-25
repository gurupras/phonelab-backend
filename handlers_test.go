package phonelab_backend

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDeviceWorkHandler(t *testing.T) {
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
	assert := assert.New(t)

	waitForPendingWorkChannel := func() {
		for {
			if PendingWorkChannel != nil {
				break
			}
		}
	}

	PendingWorkChannel = nil
	go PendingWorkHandler()
	waitForPendingWorkChannel()
	//PendingWorkChannel <- &Work{DeviceId: "dummy"}
	close(PendingWorkChannel)

	time.Sleep(3 * time.Second)

	started := 0
	verified := 0
	mutex := sync.Mutex{}
	countFn := func(work *Work) {
		mutex.Lock()
		verified++
		mutex.Unlock()
	}

	PendingWorkChannel = nil
	// We start PendingWorkHandler in a goroutine from which we can signal back that it has returned
	pendingWorkHandlerDone := make(chan int)
	go func() {
		PendingWorkHandler(countFn)
		close(pendingWorkHandlerDone)
	}()
	waitForPendingWorkChannel()

	devices := LoadDevicesFromFile("./deviceids.txt")

	wg := new(sync.WaitGroup)
	startedMutex := sync.Mutex{}
	workProducer := func(deviceId string, stopChannel chan interface{}) {
		//fmt.Println("workProducer:", deviceId)
		defer wg.Done()
		stop := false

		go func() {
			_ = <-stopChannel
			stop = true
		}()

		for {
			if stop {
				break
			}
			work := new(Work)
			work.DeviceId = deviceId
			PendingWorkChannel <- work
			startedMutex.Lock()
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

	// XXX: Remove this sleep
	time.Sleep(1 * time.Millisecond)

	// All producers have terminated. The number of tasks posted into
	// PendingWorkChannel cannot change anymore.
	// Close the PendingWorkChannel to trigger stopping of consumers
	close(PendingWorkChannel)

	// Wait for PendingWorkHandler to return signalling that all consumers
	// have finished consuming everything there is to consume
	_, _ = <-pendingWorkHandlerDone

	//Now confirm that all posted work was completed
	assert.Equal(started, verified, fmt.Sprintf("Started(%d) != Verified(%d)", started, verified))
}
