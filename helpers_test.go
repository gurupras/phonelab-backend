package phonelab_backend_test

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/google/shlex"
	"github.com/gurupras/gocommons"
	"github.com/gurupras/phonelab_backend"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/parnurzeal/gorequest"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

var (
	logger           *logrus.Logger
	testDirBase      string = os.Getenv("TEST_DIR_BASE")
	stagingDirGlobal string
	outDirGlobal     string
)

const (
	DONE    int = iota
	PENDING int = iota
)

func Recover(name string) {
	if r := recover(); r != nil {
		logger.Error(fmt.Sprintf("%s: FAILED\n%s\n%s\n", name, r, debug.Stack()))
	}
}

func DataGenerator(channel chan string, stop chan interface{}) {
	var shouldStop bool = false

	defer close(channel)

	stopMutex := sync.Mutex{}

	// Start a goroutine to listen for stop
	go func() {
		_ = <-stop
		stopMutex.Lock()
		defer stopMutex.Unlock()
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
		stopMutex.Lock()
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
		stopMutex.Unlock()
	}
}

func LoadDevicesFromFile(filePath string, assert *assert.Assertions) []string {
	devices := make([]string, 0)

	fstruct, err := gocommons.Open(filePath, os.O_RDONLY, gocommons.GZ_UNKNOWN)
	assert.Nil(err, "Failed to open file:", filePath, err)
	if err != nil {
		logger.Error("Failed to open file:", filePath)
		return nil
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
	assert.True(len(devices) > 0, "Failed to load devices from file")
	return devices
}

// Generates data and and sends it to the server via POST requests
// Each request is of random length between MIN_REQ_SIZE and MAX_REQ_SIZE
// Continues until it receives data on the quitChannel
func DeviceDataGenerator(deviceId string, port int, commChannel chan interface{}, dataRequestChannel chan interface{}, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	var (
		MIN_REQ_SIZE int = 102400 * 4
		MAX_REQ_SIZE int = 1024000
	)

	// Generators always close channels
	logChannel := make(chan string, 10000)

	dataQuitChannel := make(chan interface{})
	defer close(dataQuitChannel)

	stopMutex := new(sync.Mutex)

	// Start the data generator
	go DataGenerator(logChannel, dataQuitChannel)

	baseUrl := fmt.Sprintf("http://localhost:%d/uploader", port)
	version := "2.0.1"
	packageName := "edu.buffalo.cse.phonelab.conductor.tasks.LogcatTask"
	fileName := "logger.out"
	url := fmt.Sprintf("%s/%s/%s/%s/%s", baseUrl, version, deviceId, packageName, fileName)
	logger.Debug("URL:", url)
	// Now collect lines, package them into 'files'
	// and ship them via POST

	wg := new(sync.WaitGroup)

	for {
		logger.Debug("Waiting for data request:", deviceId)
		if _, ok := <-dataRequestChannel; !ok {
			dataQuitChannel <- struct{}{}
			break
		}

		file := new(bytes.Buffer)
		//var fileWriter io.Writer
		//fileWriter = file

		fileWriter := gzip.NewWriter(file)
		// Next file size. [MIN_REQ_SIZE, MAX_REQ_SIZE]
		randomSize := MIN_REQ_SIZE + rand.Intn(MAX_REQ_SIZE-MIN_REQ_SIZE+1)
		currentSize := 0

		for currentSize < randomSize {
			line := <-logChannel

			fileWriter.Write([]byte(line))
			currentSize += len(line)
		}
		fileWriter.Flush()
		fileWriter.Close()

		handlePayload := func(file *bytes.Buffer, size int) {
			defer wg.Done()
			stopMutex.Lock()
			defer stopMutex.Unlock()

			commChannel <- PENDING
			// Send back any data you want to
			resp, body, err := gorequest.New().Post(url).
				Set("Content-Length", fmt.Sprintf("%v", len(file.String()))).
				Set("Content-Type", "application/x-www-form-urlencoded").
				Send(file.String()).
				End()

			_ = resp
			_ = body
			_ = err

			if err != nil {
				if resp != nil {
					logger.Error(fmt.Sprintf("Failed with status:%v-%v", resp.StatusCode, resp.Status))
					return
				} else {
					logger.Error("Response is nil")
					return
				}
			}
			if resp.StatusCode != 200 {
				logger.Error("Error while doing POST")
			}

			logger.Debug(fmt.Sprintf("%s: Sent file of size: %d bytes", deviceId, currentSize))
			commChannel <- DONE
		}
		wg.Add(1)
		go handlePayload(file, currentSize)

	}
	wg.Wait()
}

// Expects server to be started
func UploadFiles(port int, nDevices, filesPerDevice int, assert *assert.Assertions) {
	devices := LoadDevicesFromFile("deviceids.txt", assert)

	wg := sync.WaitGroup{}
	deviceWorker := func(deviceId string) {
		dataRequestChannel := make(chan interface{})
		commChannel := make(chan interface{})

		go func() {
			for filesUploaded := 0; filesUploaded < filesPerDevice; filesUploaded++ {
				dataRequestChannel <- struct{}{}
				logger.Debug(fmt.Sprintf("%s - Requested: %d", deviceId, filesUploaded+1))
			}
		}()
		go func() {
			filesUploaded := 0
			for filesUploaded < filesPerDevice {
				if data, ok := <-commChannel; !ok {
					assert.Fail("Unexpected failure")
				} else {
					if data == DONE {
						filesUploaded++
						logger.Debug("Files uploaded:", filesUploaded)
					}
				}
			}
			close(dataRequestChannel)
			logger.Debug("Stopping DeviceDataGenerator")
		}()
		DeviceDataGenerator(deviceId, port, commChannel, dataRequestChannel, &wg)
	}
	for i := 0; i < nDevices; i++ {
		wg.Add(1)
		go deviceWorker(devices[i])
	}
	logger.Debug("Waiting for wg")
	wg.Wait()
	logger.Debug("wg done")
	logger.Debug("Uploaded:", nDevices*filesPerDevice)
}

func cleanup(directories ...string) {
	// We need to clean up
	logger.Debug("Cleaning up")
	var directoriesToDelete []string
	if directories == nil || len(directories) == 0 {
		directoriesToDelete = []string{stagingDirGlobal, outDirGlobal}
	} else {
		directoriesToDelete = directories
	}

	for _, dir := range directoriesToDelete {
		cmdline := fmt.Sprintf("rm -rf %v", dir)
		if args, err := shlex.Split(cmdline); err != nil {
			logger.Error("Failed to split:", cmdline)
		} else {
			if ret, stdout, stderr := gocommons.Execv(args[0], args[1:], true); ret != 0 {
				logger.Error(stdout)
				logger.Error(stderr)
			}
		}
	}
	return
}

func RunTestServerAsync(port int, config *phonelab_backend.Config, serverPtr **phonelab_backend.Server) {

	var err error
	defer Recover("RunTestServerAsync")

	if config == nil {
		config = &phonelab_backend.Config{}
		config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
	}

	if strings.Compare(config.StagingDir, "") == 0 {
		if config.StagingDir, err = ioutil.TempDir(testDirBase, "staging-"); err != nil {
			logger.Error("Failed to create staging dir")
			os.Exit(-1)
		}
	}

	if strings.Compare(config.OutDir, "") == 0 {
		if config.OutDir, err = ioutil.TempDir(testDirBase, "outdir-"); err != nil {
			logger.Error("Failed to create outdir")
			os.Exit(-1)
		}
	}

	stagingDirGlobal = config.StagingDir
	outDirGlobal = config.OutDir

	// FIXME: Port should probably be part of command line arguments
	// or a config file. Currently, this is hard-coded.
	logger.Debug("Starting server ...")

	// XXX: Should pending work handler be included here?
	// If not, we would have to include it in every place that
	// is looking to test an upload (currently, all tests in this file)
	pendingWorkHandlerWg := sync.WaitGroup{}
	pendingWorkHandlerWg.Add(1)
	go func() {
		defer pendingWorkHandlerWg.Done()
		phonelab_backend.PendingWorkHandler(config)
	}()
	*serverPtr, err = phonelab_backend.SetupServer(port, config, false)
	if err != nil {
		panic(err)
	}
	logger.Debug("Running server on port:", port)
	phonelab_backend.RunServer(*serverPtr)
	logger.Debug("Attempting to close work channel")
	config.CloseWorkChannel()
	pendingWorkHandlerWg.Wait()
	logger.Debug("Closed server")
}

func TestDeviceDataGeneratorCompression(t *testing.T) {
	// We test whether the data generated by DeviceDataGenerator is compressed
	// Setup a server with custom upload handler
	t.Parallel()

	assert := assert.New(t)

	var port int = 21356
	var err error
	var server *phonelab_backend.Server
	var config *phonelab_backend.Config

	config = new(phonelab_backend.Config)
	config.StagingDir, err = ioutil.TempDir(testDirBase, "staging-")
	assert.Nil(err, "Failed to create staging dir")

	config.OutDir, err = ioutil.TempDir(testDirBase, "outdir-")
	assert.Nil(err, "Failed to create outdir")

	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)

	compressionChecker := func(c echo.Context) error {
		body := c.Request().Body()
		var compressedReader *gzip.Reader
		var err error

		compressedReader, err = gzip.NewReader(body)
		if err != nil {
			assert.Nil(err, "Body is not compressed:", err.Error())
			reader := bufio.NewScanner(body)
			reader.Split(bufio.ScanLines)
			for reader.Scan() {
				line := reader.Text()
				logger.Debug(line)
			}
		}

		_ = compressedReader

		err = c.String(200, "OK")
		// We've responded. Now shut down the server
		server.Stop()
		return err
	}

	serverWg := sync.WaitGroup{}
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		server, err = phonelab_backend.New(port)
		assert.Nil(err, "Failed to create server", err)
		server.Use(middleware.Logger())
		// Set up the routes
		server.POST("/uploader/:version/:deviceId/:packageName/:fileName", compressionChecker)
		server.Run()
	}()
	UploadFiles(port, 1, 1, assert)
	serverWg.Wait()
}

func TestMain(m *testing.M) {
	logger = logrus.New()
	logger.Level = logrus.InfoLevel

	if strings.Compare(testDirBase, "") == 0 {
		testDirBase = "/tmp"
	}
	if exists, err := gocommons.Exists(testDirBase); err != nil || !exists {
		gocommons.Makedirs(testDirBase)
	}

	logger.Debug("testDirBase:", testDirBase)

	code := m.Run()
	os.Exit(code)
}
