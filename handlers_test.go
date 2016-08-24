package phonelab_backend

import (
	"sync"
	"testing"
	"time"
)

func TestDeviceWorkHandler(t *testing.T) {
	channel := make(chan *Work)
	close(channel)
	go DeviceWorkHandler("dummy", channel)

	//XXX: If we didn't crash, we were fine

	channel = make(chan *Work)
	go DeviceWorkHandler("dummy", channel)

	for i := 0; i < 1000; i++ {
		channel <- &Work{}
	}
}

func TestPendingWorkHandler(t *testing.T) {
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

	PendingWorkChannel = nil
	go PendingWorkHandler()
	waitForPendingWorkChannel()

	devices := LoadDevicesFromFile("./deviceids.txt")

	wg := new(sync.WaitGroup)
	deviceHandler := func(deviceId string, stopChannel chan interface{}) {
		//fmt.Println("deviceHandler:", deviceId)
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
		}
	}

	stopChannel := make(chan interface{})

	for idx := 0; idx < 10; idx++ {
		wg.Add(1)
		go deviceHandler(devices[idx], stopChannel)
	}

	time.Sleep(5 * time.Second)

	for idx := 0; idx < 10; idx++ {
		stopChannel <- struct{}{}
	}
	wg.Wait()
}
