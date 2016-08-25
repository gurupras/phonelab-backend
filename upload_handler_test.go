package phonelab_backend

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/shlex"
	"github.com/gurupras/gocommons"
)

const (
	DONE    int = iota
	PENDING int = iota
)

var (
	server *Server
)

func cleanup() {
	// We need to clean up
	fmt.Println("Cleaning up")
	directoriesToDelete := []string{StagingDirBase, OutDirBase}
	for _, dir := range directoriesToDelete {
		cmdline := fmt.Sprintf("rm -rf %v", dir)
		if args, err := shlex.Split(cmdline); err != nil {
			fmt.Fprintln(os.Stderr, "Failed to split:", cmdline)
		} else {
			if ret, stdout, stderr := gocommons.Execv(args[0], args[1:], true); ret != 0 {
				fmt.Fprintln(os.Stderr, stdout)
				fmt.Fprintln(os.Stderr, stderr)
			}
		}
	}
	return
}

func TestUpload(t *testing.T) {
	device := "dummy-TestUpload"

	go RunTestServerAsync(8083)

	stopChannel := make(chan interface{})
	commChannel := make(chan interface{})
	defer close(commChannel)

	go func() {
		// Dummy consumer for commChannel
		for {
			if _, ok := <-commChannel; !ok {
				break
			}
		}
	}()

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go DeviceDataGenerator(device, 8083, commChannel, stopChannel, wg)
	// We stop after 1 upload
	stopChannel <- <-commChannel
	wg.Wait()

	server.Stop()
}

func RunTestServerAsync(port int) {
	var err error
	if StagingDirBase, err = ioutil.TempDir("/tmp", "staging-"); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to create staging dir")
		os.Exit(-1)
	}

	if OutDirBase, err = ioutil.TempDir("/tmp", "outdir-"); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to create outdir")
		os.Exit(-1)
	}

	// FIXME: Port should probably be part of command line arguments
	// or a config file. Currently, this is hard-coded.
	//fmt.Println("Starting server ...")

	// XXX: Should pending work handler be included here?
	// If not, we would have to include it in every place that
	// is looking to test an upload (currently, all tests in this file)
	go PendingWorkHandler()
	server, err = SetupServer(port, true)
	if err != nil {
		panic(err)
	}
	RunServer(server)
}

func TestLoadCapability(t *testing.T) {
	time.Sleep(3 * time.Second)
	go RunTestServerAsync(8084)

	devices := LoadDevicesFromFile("deviceids.txt")

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
