package phonelab_backend_test

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gurupras/gocommons"
	"github.com/gurupras/gocommons/seekable_stream"
	"github.com/gurupras/phonelab_backend"
	"github.com/jehiah/go-strftime"
	"github.com/pbnjay/strptime"
	"github.com/stretchr/testify/assert"
)

func errNoFail(work *phonelab_backend.Work) (err error, fail bool) {
	fail = false
	err = errors.New("Expected")
	return
}

func errAndFail(work *phonelab_backend.Work) (err error, fail bool) {
	fail = true
	err = errors.New("Expected")
	return
}

func TestStaging(t *testing.T) {
	//t.Parallel()

	var port int = 8083
	var server *phonelab_backend.Server
	assert := assert.New(t)

	defer Recover("TestStaging", assert)

	dummyWork := func(work *phonelab_backend.DeviceWork, processingConfig *phonelab_backend.ProcessingConfig) (err error) {
		// Dummy work function. We're only testing whether server
		// correctly receives upload and stages it
		return
	}

	config := new(phonelab_backend.Config)

	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)

	config.StagingConfig = phonelab_backend.InitializeStagingConfig()
	config.ProcessingConfig = new(phonelab_backend.ProcessingConfig)
	config.ProcessingConfig.Core = dummyWork

	go RunTestServerAsync(port, config, &server)

	UploadFiles(port, 3, 5, assert)
	server.Stop()
	cleanup()
}

func TestMakeStagedFileReadOnly(t *testing.T) {
	//t.Parallel()
	assert := assert.New(t)

	defer Recover("TestMakeStagedFilesPending", assert)

	work := new(phonelab_backend.Work)
	work.StagingFileName = "/tmp/thisfiledoesnotexist"
	err, _ := phonelab_backend.MakeStagedFileReadOnly(work)
	assert.NotNil(err, "Non-exitent file was made read-only")
}

func TestUpdateStagingMetadata(t *testing.T) {
	//t.Parallel()
	assert := assert.New(t)

	defer Recover("TestUpdateStagingMetadata", assert)

	var err error
	var stagingDir string

	defer Recover("TestUpdateStagingMetadata", assert)

	// UpdateStagingMetadata requires a work struct to be passed in.
	// In particular, Work.StagingFileName. So go ahead and create one.
	work := new(phonelab_backend.Work)

	stagingDir, err = ioutil.TempDir(testDirBase, "staging-")
	assert.Nil(err, "Failed to create staging dir", err)

	var file *os.File
	file, err = ioutil.TempFile(stagingDir, "updateStagingMetadata-")
	assert.Nil(err, "Failed to create temporary staging file", err)
	file.Close()
	os.Remove(file.Name())

	work.StagingFileName = file.Name()

	// It also requires Work.DataStream. So add some (compressed) data to it
	buf := new(bytes.Buffer)
	gzipWriter := gzip.NewWriter(buf)
	line := GenerateLoglineForPayload("dummy data")
	gzipWriter.Write([]byte(line))
	gzipWriter.Flush()
	gzipWriter.Close()

	work.DataStream = new(seekable_stream.SeekableStream)
	work.DataStream.WrapBytes(buf.Bytes())

	// Now, create this file and make it read only so UpdateStagingMetadata will fail
	file, err = os.OpenFile(work.StagingFileName, os.O_CREATE|os.O_RDONLY, 0400)
	assert.Nil(err, "Failed to create staging file", err)
	file.Close()
	err, _ = phonelab_backend.UpdateStagingMetadata(work)
	assert.NotNil(err, "Should've failed with read only file")
	os.Remove(file.Name())

	file, err = os.OpenFile(work.StagingFileName, os.O_CREATE|os.O_RDWR, 0644)
	assert.Nil(err, "Failed to create staging file", err)
	file.Close()
	err, _ = phonelab_backend.UpdateStagingMetadata(work)
	assert.Nil(err, "Failed with valid arguments:", err)
}

func TestRunStagingProcesses(t *testing.T) {
	//t.Parallel()
	assert := assert.New(t)

	defer Recover("TestRunStagingProcesses", assert)

	var processes []phonelab_backend.StagingProcess
	var errs []error
	var fail bool
	// First do no failure condition
	processes = []phonelab_backend.StagingProcess{errNoFail}
	errs, fail = phonelab_backend.RunStagingProcesses(processes, nil)
	assert.Equal(1, len(errs), "Should have got one error")
	assert.False(fail, "Should have been false")

	// Just for completeness, do multiple processes
	processes = []phonelab_backend.StagingProcess{errNoFail, errNoFail}
	errs, fail = phonelab_backend.RunStagingProcesses(processes, nil)
	assert.Equal(2, len(errs), "Should have got two errors")
	assert.False(fail, "Should have been false")

	// Now, do the failure condition
	processes = []phonelab_backend.StagingProcess{errAndFail}
	errs, fail = phonelab_backend.RunStagingProcesses(processes, nil)
	assert.Equal(1, len(errs), "Should have got one error")
	assert.True(fail, "Should have been true")

	// Now, multiple
	processes = []phonelab_backend.StagingProcess{errNoFail, errAndFail}
	errs, fail = phonelab_backend.RunStagingProcesses(processes, nil)
	assert.Equal(2, len(errs), "Should have got two errors")
	assert.True(fail, "Should have been true")

	processes = []phonelab_backend.StagingProcess{errAndFail, errNoFail}
	errs, fail = phonelab_backend.RunStagingProcesses(processes, nil)
	assert.Equal(1, len(errs), "Should have got one error")
	assert.True(fail, "Should have been true")
}

func TestCreateStagingFile(t *testing.T) {
	//t.Parallel()
	assert := assert.New(t)

	defer Recover("TestCreateStagingFile", assert)

	var stagingDir string
	var err error

	stagingDir, err = ioutil.TempDir(testDirBase, "staging-")
	assert.Nil(err, "Failed to create staging dir", err)
	gocommons.Makedirs(stagingDir)
	os.Chmod(stagingDir, 0555)

	work := new(phonelab_backend.Work)
	work.StagingDir = stagingDir
	err, _ = phonelab_backend.CreateStagingFile(work)
	assert.NotNil(err, "Should have failed to create file inside read only directory")
}

func TestAddStagingMetadata(t *testing.T) {
	//t.Parallel()
	assert := assert.New(t)

	defer Recover("TestAddStagingMetadata", assert)

	stagingDirBase := filepath.Join(testDirBase, "staging-test-add-metadata/")
	outDirBase := filepath.Join(testDirBase, "out-test-add-metadata/")

	port := 8086
	var server *phonelab_backend.Server
	config := new(phonelab_backend.Config)
	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
	config.StagingDir = stagingDirBase
	config.OutDir = outDirBase

	config.ProcessingConfig = new(phonelab_backend.ProcessingConfig)
	dummyWork := func(work *phonelab_backend.DeviceWork, processingConfig *phonelab_backend.ProcessingConfig) (err error) {
		// Do nothing. We're only testing adding staging metadata
		return
	}
	config.ProcessingConfig.Core = dummyWork

	go RunTestServerAsync(port, config, &server)
	UploadFiles(port, 1, 1, assert)
	server.Stop()
	cleanup(stagingDirBase, outDirBase)
}

func TestHandleUpload(t *testing.T) {
	//t.Parallel()
	assert := assert.New(t)

	defer Recover("TestHandleUpload", assert)

	var err error
	var file *os.File
	file, err = gocommons.TempFile(testDirBase, "staging-", ".txt")
	assert.Nil(err, "Failed to create temporary file:", err)
	file.Close()

	work := new(phonelab_backend.Work)

	var stagingConfig *phonelab_backend.StagingConfig

	compressedBuf := new(bytes.Buffer)
	writer := gzip.NewWriter(compressedBuf)
	line := GenerateLoglineForPayload("Hello, world!")
	writer.Write([]byte(line))
	writer.Flush()
	writer.Close()
	compressedBytes := compressedBuf.Bytes()

	// First, fail pre-processing
	stagingConfig = new(phonelab_backend.StagingConfig)
	stagingConfig.PreProcessing = append(stagingConfig.PreProcessing, errNoFail)
	stagingConfig.PreProcessing = append(stagingConfig.PreProcessing, errAndFail)

	_, err = phonelab_backend.HandleUpload(new(bytes.Buffer), nil, nil, stagingConfig)
	assert.NotNil(err, "Should have errored")
	stagingConfig.PreProcessing = stagingConfig.PreProcessing[:0]

	// Now, for coverage, run a bunch of preprocessing that throw errors but don't fail
	stagingConfig.PreProcessing = append(stagingConfig.PreProcessing, errNoFail)
	stagingConfig.PreProcessing = append(stagingConfig.PreProcessing, errNoFail)
	_, err = phonelab_backend.HandleUpload(new(bytes.Buffer), work, nil, stagingConfig)
	// Core processing will still fail.
	assert.NotNil(err, "Should have errored")
	stagingConfig.PreProcessing = stagingConfig.PreProcessing[:0]
	// TODO: Now, fail core

	stagingConfig = new(phonelab_backend.StagingConfig)
	uncompressedBuf := new(bytes.Buffer)
	uncompressedBuf.WriteString(line)
	work.DataStream = new(seekable_stream.SeekableStream)
	work.DataStream.WrapReader(uncompressedBuf)

	work.StagingFileName = "/tmp/doesnotexist"
	_, err = phonelab_backend.HandleUpload(uncompressedBuf, work, nil, stagingConfig)
	assert.NotNil(err, "Should have failed on non-existent file")

	// Now, fail post-processing
	work.StagingFileName = file.Name()

	work.DataStream = new(seekable_stream.SeekableStream)
	work.DataStream.WrapBytes(compressedBytes)

	stagingConfig = new(phonelab_backend.StagingConfig)
	stagingConfig.PostProcessing = append(stagingConfig.PostProcessing, errNoFail)
	stagingConfig.PostProcessing = append(stagingConfig.PostProcessing, errAndFail)
	_, err = phonelab_backend.HandleUpload(compressedBuf, work, nil, stagingConfig)
	assert.NotNil(err, "Should have errored")
	stagingConfig.PostProcessing = stagingConfig.PostProcessing[:0]

	// Now succeed
	stagingConfig = new(phonelab_backend.StagingConfig)
	stagingConfig.PostProcessing = append(stagingConfig.PostProcessing, errNoFail)
	stagingConfig.PostProcessing = append(stagingConfig.PostProcessing, errNoFail)
	dummyWorkChannel := make(chan *phonelab_backend.Work, 1000)

	work.StagingDir = "/tmp/staging-TestHandleUpload"
	work.StagingFileName = file.Name()
	work.DataStream = new(seekable_stream.SeekableStream)
	work.DataStream.WrapBytes(compressedBytes)

	_, err = phonelab_backend.HandleUpload(compressedBuf, work, dummyWorkChannel, stagingConfig)
	assert.Nil(err, "Should not have errored", err)
	stagingConfig.PostProcessing = stagingConfig.PostProcessing[:0]
}

func TestHandleUploaderPost(t *testing.T) {
	//t.Parallel()
	assert := assert.New(t)

	defer Recover("TestHandleUploaderPost", assert)

	var err error
	var port int = 11781
	var server *phonelab_backend.Server

	config := new(phonelab_backend.Config)

	config.StagingDir, err = ioutil.TempDir(testDirBase, "staging-")
	assert.Nil(err, "Failed to create temporary staging dir")

	config.OutDir, err = ioutil.TempDir(testDirBase, "outdir-")
	assert.Nil(err, "Failed to create temporary outdir")

	// First, test fail condition
	config.StagingConfig = new(phonelab_backend.StagingConfig)
	config.StagingConfig.PreProcessing = append(config.StagingConfig.PreProcessing, errAndFail)

	server, err = phonelab_backend.SetupServer(port, config, true)
	assert.Nil(err, "Failed to start server:", err)

	go server.Run()

	// Now we upload something that we know will cause an error..uncompressed
	resp, _, errs := Upload(port, "dummy-device", "hello")
	assert.Zero(len(errs), "Failed to upload data to server:", errs)
	assert.NotEqual(200, resp.StatusCode, "Should have received code other than 200")
	server.Stop()
	cleanup(config.StagingDir, config.OutDir)

	err = gocommons.Makedirs(config.StagingDir)
	assert.Nil(err, "Failed to create staging dir")
	err = gocommons.Makedirs(config.OutDir)
	assert.Nil(err, "Failed to create out dir")

	// Now, test success
	config.StagingConfig = phonelab_backend.InitializeStagingConfig()
	config.ProcessingConfig = phonelab_backend.InitializeProcessingConfig()
	port++
	server, err = phonelab_backend.SetupServer(port, config, true)
	go server.Run()

	buf := new(bytes.Buffer)
	line := GenerateLoglineForPayload("Hello")
	compressedWriter := gzip.NewWriter(buf)
	compressedWriter.Write([]byte(line))
	compressedWriter.Flush()
	compressedWriter.Close()

	resp, _, errs = Upload(port, "dummy-device", buf.String())
	assert.Equal(200, resp.StatusCode, "Should have received code 200")
	server.Stop()
	cleanup(config.StagingDir, config.OutDir)
}

func TestUpload(t *testing.T) {
	//t.Parallel()

	assert := assert.New(t)

	defer Recover("TestUpload", assert)

	var port int = 8084
	var server *phonelab_backend.Server
	var nDevices int = 1
	var nFilesPerDevice int = 5

	config := new(phonelab_backend.Config)

	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
	config.StagingConfig = phonelab_backend.InitializeStagingConfig()
	config.ProcessingConfig = phonelab_backend.InitializeProcessingConfig()
	config.ProcessingConfig.WorkSetCheckPeriod = 1 * time.Second
	config.ProcessingConfig.DelayBeforeProcessing = 3 * time.Second

	count := int32(0)
	expected := int32(nDevices * nFilesPerDevice)
	// Create a waitgroup that we will use to signal back
	wg := sync.WaitGroup{}
	wg.Add(1)
	countFn := func(work *phonelab_backend.DeviceWork) (err error, fail bool) {
		atomic.AddInt32(&count, int32(len(work.WorkList)))
		logger.Debugln(fmt.Sprintf("Counted: %v", count))
		if count == expected {
			wg.Done()
		}
		return
	}
	config.ProcessingConfig.PostProcessing = append(config.ProcessingConfig.PostProcessing, countFn)

	go RunTestServerAsync(port, config, &server)

	UploadFiles(port, nDevices, nFilesPerDevice, assert)
	// Wait for the callback to signal that we're done
	wg.Wait()
	server.Stop()
	assert.Equal(expected, count, "Did not process expected # of uploaded files")
	cleanup()
}

func TestLoadCapability(t *testing.T) {
	//t.Parallel()

	assert := assert.New(t)

	defer Recover("TestLoadCapability", assert)

	t.Skip("TestLoadCapability: Skipping until logic for evaluating output is decided")

	var port int = 8085
	var server *phonelab_backend.Server

	defer Recover("TestLoadCapability", assert)

	config := new(phonelab_backend.Config)
	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
	go RunTestServerAsync(port, config, &server)

	devices := LoadDevicesFromFile("deviceids.txt", assert)

	nDevices := 30

	commChannel := make(chan interface{}, 100)

	// Function to track pending workloads
	go func() {
		pending := 0

		// Goroutine to periodically print pending uploads
		go func() {
			for {
				time.Sleep(1 * time.Second)
				logger.Info("TLC: Pending uploads:", pending)
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
					logger.Error("What is this? %v", state)
				}
			}
		}
	}()

	wg := new(sync.WaitGroup)

	start := time.Now().UnixNano()
	testTimeNanos := int64(30 * 1e9)

	dataGenerator := func(deviceId string) {
		dataRequestChannel := make(chan interface{})
		go func() {
			for {
				now := time.Now().UnixNano()
				if now-start > testTimeNanos {
					break
				}
				// Generate requests as fast as possible
				// Therefore, no sleeps
				dataRequestChannel <- struct{}{}
			}
			close(dataRequestChannel)
		}()
		DeviceDataGenerator(deviceId, port, commChannel, dataRequestChannel, wg)
	}

	for idx := 0; idx < nDevices; idx++ {
		device := devices[idx]
		wg.Add(1)
		go dataGenerator(device)
	}

	logger.Debug("Waiting to terminate ...")
	wg.Wait()
	logger.Debug("Terminating ...")

	logger.Debug("Stopping server ...")
	//TODO: Server stop logic
	server.Stop()
	//cleanup()
}

func TestDateMap(t *testing.T) {
	//t.Parallel()

	assert := assert.New(t)

	now := time.Now()

	dt := strptime.MustParse(strftime.Format("%Y-%M-%d", now), "%Y-%M-%d")
	dm := make(map[time.Time]string)

	dm[dt] = "exists"

	new_dt := strptime.MustParse(strftime.Format("%Y-%M-%d", now), "%Y-%M-%d")

	val, ok := dm[new_dt]
	assert.True(ok, "Does not exist in map")
	assert.Equal("exists", val, "Value did not match")
}

func TestSplitUploadToChunks(t *testing.T) {
	//t.Parallel()

	assert := assert.New(t)

	defer Recover("TestUpload", assert)

	var port int = 8087
	var server *phonelab_backend.Server

	// Set up a server with custom core function
	config := new(phonelab_backend.Config)

	config.WorkChannel = make(chan *phonelab_backend.Work)
	config.ProcessingConfig = phonelab_backend.InitializeProcessingConfig()
	config.ProcessingConfig.WorkSetCheckPeriod = 1 * time.Second
	config.ProcessingConfig.DelayBeforeProcessing = 48 * time.Hour

	count := int32(0)
	countFn := func(work *phonelab_backend.DeviceWork, processingConfig *phonelab_backend.ProcessingConfig) (err error) {
		logger.Infoln("Counted:", len(work.WorkList))
		atomic.AddInt32(&count, int32(len(work.WorkList)))
		if count == 2 {
			server.Stop()
		}
		return
	}
	config.ProcessingConfig.Core = countFn

	// Create a waitgroup and increment its count
	// The countFn is responsible for signalling back by closing the server
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		RunTestServerAsync(port, config, &server)
	}()

	data := new(bytes.Buffer)
	gzipWriter := gzip.NewWriter(data)

	rlg1 := new(RandomLoglineGenerator)
	rlg1.BootId = GenerateRandomBootId()
	rlg1.StartTimestamp = strptime.MustParse("2016-01-01", "%Y-%m-%d")
	rlg1.LastLogcatToken = 0
	rlg1.LastLogcatTimestamp = rlg1.StartTimestamp
	rlg1.MaxDelayBetweenLoglines = 100 * time.Millisecond

	gzipWriter.Write([]byte(GenerateRandomLogline(rlg1)))

	rlg2 := new(RandomLoglineGenerator)
	rlg2.BootId = GenerateRandomBootId()
	rlg2.StartTimestamp = strptime.MustParse("2016-01-04", "%Y-%m-%d")
	rlg2.LastLogcatToken = 0
	rlg2.LastLogcatTimestamp = rlg2.StartTimestamp
	rlg2.MaxDelayBetweenLoglines = 100 * time.Millisecond

	gzipWriter.Write([]byte(GenerateRandomLogline(rlg2)))
	gzipWriter.Flush()
	gzipWriter.Close()

	// We now have 1 'file' containing logs from 2 dates.
	// Upload this to check for correct split logic
	// This is done by the count function
	// The count function should result in a count of 2 for an upload count of 1
	// You can see where we're going with this...
	Upload(port, "dummy", data.String())
	wg.Wait()
	assert.Equal(int32(2), count, "Did not process expected # of uploaded files")
	cleanup()
}
