package phonelab_backend

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/shlex"
	"github.com/gurupras/gocommons"
	"github.com/parnurzeal/gorequest"
	"github.com/satori/go.uuid"
)

const (
	DONE    int = iota
	PENDING int = iota
)

func DataGenerator(channel chan string, stop chan interface{}) {
	var shouldStop bool = false

	defer close(channel)

	// Start a goroutine to listen for stop
	go func() {
		_ = <-stop
		shouldStop = true
	}()

	var logcatToken int64
	var now int64
	var bootId string

	reset := func() {
		bootId = uuid.NewV4().String()
		logcatToken = 0
		now = time.Now().UnixNano()
	}

	reset()
	for {
		if shouldStop {
			break
		}
		// There is a 1-in-a-million chance of changing boot IDs
		if rand.Intn(10000000) == 0 {
			reset()
		}

		// Generate a logline
		line := GenerateRandomLogline(bootId, now, logcatToken)
		channel <- line
		logcatToken++
	}
}

func LoadDevicesFromFile(filePath string) []string {
	devices := make([]string, 0)

	fstruct, err := gocommons.Open(filePath, os.O_RDONLY, gocommons.GZ_UNKNOWN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to open file:", filePath)
		os.Exit(-1)
	}

	channel := make(chan string)

	go fstruct.AsyncRead(bufio.ScanLines, channel)

	for {
		device, ok := <-channel
		if !ok {
			break
		}
		devices = append(devices, device)
	}
	return devices
}

// Generates data and and sends it to the server via POST requests
// Each request is of random length between MIN_REQ_SIZE and MAX_REQ_SIZE
// Continues until it receives data on the quitChannel
func DeviceDataGenerator(deviceId string, commChannel chan interface{}, quitChannel chan interface{}, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	var (
		MIN_REQ_SIZE int = 102400 * 8
		MAX_REQ_SIZE int = 1024000
	)

	// Generators always close channels
	logChannel := make(chan string, 10000)

	stop := false

	dataQuitChannel := make(chan interface{})
	defer close(dataQuitChannel)

	stopMutex := new(sync.Mutex)
	go func() {
		dataQuitChannel <- <-quitChannel
		stopMutex.Lock()
		defer stopMutex.Unlock()
		stop = true
	}()

	// Start the data generator
	go DataGenerator(logChannel, dataQuitChannel)

	baseUrl := "http://dirtydeeds.cse.buffalo.edu:8000/phonelab/uploader"
	version := "2.0.1"
	packageName := "edu.buffalo.cse.phonelab.conductor.tasks.LogcatTask"
	fileName := "log.out"
	url := fmt.Sprintf("%s/%s/%s/%s/%s", baseUrl, version, deviceId, packageName, fileName)
	//fmt.Println("URL:", url)
	// Now collect lines, package them into 'files'
	// and ship them via POST

	wg := new(sync.WaitGroup)

	for {
		if stop {
			break
		}
		var file bytes.Buffer
		// Next file size. [MIN_REQ_SIZE, MAX_REQ_SIZE]
		randomSize := MIN_REQ_SIZE + rand.Intn(MAX_REQ_SIZE-MIN_REQ_SIZE+1)
		currentSize := 0

		for currentSize < randomSize {
			if stop {
				break
			}
			line := <-logChannel

			file.WriteString(line)
			currentSize += len(line)
		}

		handlePayload := func(payload string, size int) {
			defer wg.Done()
			stopMutex.Lock()
			defer stopMutex.Unlock()
			if stop {
				return
			}
			commChannel <- PENDING
			// Send back any data you want to
			resp, body, err := gorequest.New().Post(url).
				Set("Content-Length", fmt.Sprintf("%v", size)).
				Set("Accept-Encoding", "gzip").
				Set("Content-Type", "application/x-www-form-urlencoded").
				Send(payload).
				End()
			commChannel <- DONE

			_ = resp
			_ = body
			_ = err

			if err != nil {
				if resp != nil {
					fmt.Fprintln(os.Stderr, fmt.Sprintf("Failed with status:%v-%v", resp.StatusCode, resp.Status))
				} else {
					fmt.Fprintln(os.Stderr, "Response is nil")
				}
			}
		}
		wg.Add(1)
		go handlePayload(file.String(), currentSize)
		//fmt.Println(resp)

		//fmt.Println(fmt.Sprintf("%s: Sent file of size: %d bytes", deviceId, currentSize))
	}
	wg.Wait()
}

func TestUpload(t *testing.T) {
	device := "dummy-TestUpload"

	result := gocommons.InitResult("TestUpload")

	serverStopChannel := make(chan interface{})
	serverCallbackChannel := make(chan interface{})
	go RunTestServerAsync(serverStopChannel, serverCallbackChannel)

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
	go DeviceDataGenerator(device, commChannel, stopChannel, wg)
	// We stop after 1 upload
	stopChannel <- <-commChannel
	wg.Wait()

	// Close server
	serverStopChannel <- struct{}{}
	_ = <-serverCallbackChannel

	time.Sleep(1 * time.Second)

	gocommons.HandleResult(t, true, result)
}

func RunTestServerAsync(stopChannel chan interface{}, callbackChannel chan interface{}) (err error) {
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

	go RunServer(8081, true)

	//fmt.Println("Waiting to clean up")
	_ = <-stopChannel
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
	callbackChannel <- struct{}{}
	return
}

func TestLoadCapability(t *testing.T) {
	result := gocommons.InitResult("TestLoadCapability")

	serverStopChannel := make(chan interface{})
	serverCallbackChannel := make(chan interface{})
	go RunTestServerAsync(serverStopChannel, serverCallbackChannel)

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
		go DeviceDataGenerator(device, commChannel, deviceChannel, wg)
	}

	time.Sleep(30 * time.Second)

	fmt.Println("Terminating ...")
	for _, channel := range channels {
		channel <- struct{}{}
	}
	wg.Wait()

	serverStopChannel <- struct{}{}
	_ = <-serverCallbackChannel

	time.Sleep(1 * time.Second)
	gocommons.HandleResult(t, true, result)
}
