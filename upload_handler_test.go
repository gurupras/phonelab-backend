package phonelab_backend

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUpload(t *testing.T) {
	var server *Server
	assert := assert.New(t)

	go RunTestServerAsync(8083, &server)

	UploadFiles(8083, 1, 1, assert)
}

func TestLoadCapability(t *testing.T) {
	var server *Server
	assert := assert.New(t)

	go RunTestServerAsync(8084, &server)

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
				fmt.Println("Pending uploads:", pending)
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
}
