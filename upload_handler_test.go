package phonelab_backend

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

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
func DeviceDataGenerator(deviceId string, commChannel chan interface{}, quitChannel chan interface{}) {
	defer close(commChannel)

	var (
		MIN_REQ_SIZE int = 102400 * 8
		MAX_REQ_SIZE int = 1024000
	)

	// Generators always close channels
	logChannel := make(chan string, 10000)

	stop := false

	dataQuitChannel := make(chan interface{})
	defer close(dataQuitChannel)

	go func() {
		dataQuitChannel <- <-quitChannel
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
	for {
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

		go func(payload string, size int) {
			// Send back any data you want to
			commChannel <- PENDING
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
		}(file.String(), currentSize)
		//fmt.Println(resp)

		//fmt.Println(fmt.Sprintf("%s: Sent file of size: %d bytes", deviceId, currentSize))

	}
}

func TestUpload(t *testing.T) {
	device := "dummy-TestUpload"

	stopChannel := make(chan interface{})
	commChannel := make(chan interface{})

	go DeviceDataGenerator(device, commChannel, stopChannel)
	// We stop after 1 upload
	stopChannel <- <-commChannel
}

func TestLoadCapability(t *testing.T) {
	devices := LoadDevicesFromFile("deviceids.txt")

	nDevices := 20

	commChannel := make(chan interface{}, 100)

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
			object := <-commChannel
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
	}()

	for idx := 0; idx < nDevices; idx++ {
		device := devices[idx]

		deviceChannel := make(chan interface{})
		channels = append(channels, deviceChannel)
		go DeviceDataGenerator(device, commChannel, deviceChannel)
	}

	time.Sleep(15 * time.Second)

	fmt.Println("Terminating ...")
	for _, channel := range channels {
		channel <- struct{}{}
	}
}
