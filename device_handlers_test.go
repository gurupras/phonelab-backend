package phonelab_backend_test

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/gurupras/gocommons"
	"github.com/gurupras/phonelab_backend"
	"github.com/stretchr/testify/assert"
)

func TestProcessStagedWork(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	defer Recover("TestProcessStagedWork")

	var port int = 11929
	var server *phonelab_backend.Server

	var (
		started         int = 0
		verified        int = 0
		nDevices        int = 5
		nFilesPerDevice int = 5
		totalFiles      int = nDevices * nFilesPerDevice
	)

	phonelab_backend.InitializeProcessingSteps()

	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}

	preProcess := func(w *phonelab_backend.DeviceWork) (err error) {
		//fmt.Println("preProcess")
		mutex.Lock()
		started++
		mutex.Unlock()
		return
	}
	postProcess := func(w *phonelab_backend.DeviceWork) (err error) {
		mutex.Lock()
		verified++
		//fmt.Println("Verified:", verified)
		if verified == totalFiles {
			wg.Done()
		}
		mutex.Unlock()
		return
	}
	customWorkFn := func(w *phonelab_backend.Work) {
		phonelab_backend.ProcessStagedWork(w)
	}

	phonelab_backend.PreProcessing = append(phonelab_backend.PreProcessing, preProcess)
	phonelab_backend.PostProcessing = append(phonelab_backend.PostProcessing, postProcess)

	wg.Add(1)

	config := new(phonelab_backend.Config)
	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
	serverWg := sync.WaitGroup{}
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		RunTestServerAsync(port, config, &server, customWorkFn)
	}()
	UploadFiles(port, nDevices, nFilesPerDevice, assert)

	wg.Wait()
	server.Stop()
	serverWg.Wait()
	cleanup()

	assert.Equal(started, verified, fmt.Sprintf("Started(%d) != verified(%d)", started, verified))
}

func TestOpenFileAndReader(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	defer Recover("TestOpenFileAndReader")

	var fstruct *gocommons.File
	var reader *bufio.Scanner
	var err error

	// Should fail
	fstruct, reader, err = phonelab_backend.OpenFileAndReader("/deadbeef")
	assert.Nil(fstruct, "No error on non-existent file")
	assert.Nil(reader, "No error on non-existent file")
	assert.NotNil(err, "No error on non-existent file")

	// Fail to read write-only file
	f, err := ioutil.TempFile("/tmp", "wronly-")
	assert.Nil(err, "Failed to create temporary write-only file")
	f.Chmod(0222)
	fstruct, reader, err = phonelab_backend.OpenFileAndReader(f.Name())
	assert.Nil(fstruct, "No error on write-only file")
	assert.Nil(reader, "No error on write-only file")
	assert.NotNil(err, "No error on write-only file")

	// Now succeed on proper file
	f.Chmod(0666)
	fstruct, reader, err = phonelab_backend.OpenFileAndReader(f.Name())
	assert.NotNil(fstruct, "Error on proper file")
	assert.NotNil(reader, "Error on proper file")
	assert.Nil(err, "Error on proper file")
	fstruct.Close()

	f.Close()
	os.Remove(f.Name())
}

/*
func TestReadFileInReverse(t *testing.T) {
	assert := assert.New(t)

	port := 21331

	workChannel := make(chan *phonelab_backend.Work)
	var server *phonelab_backend.Server
	var work *phonelab_backend.Work
	wg := sync.WaitGroup{}

	workFn := func(w *phonelab_backend.Work) {
		work = w
		server.Stop()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		RunTestServerAsync(port, workChannel, &server, workFn)
	}()

	UploadFiles(port, 1, 1, assert)
	wg.Wait()
	// Now the server is down and we have a file uploaded
	fpath := work.StagingFileName
	os.Chmod(fpath, 0666)
	fstruct, err := gocommons.Open(fpath, os.O_RDWR, gocommons.GZ_TRUE)
	assert.Nil(err, "Failed to open file from work", err)
	assert.NotNil(fstruct, "Failed to open file from work")

	fstruct.Seek(0, os.SEEK_END)
	writer, err := fstruct.Writer(0)
	assert.Nil(err, "Failed to get writer", err)
	assert.NotNil(writer, "Failed to get writer")

	// Now write a few lines that we know
	var nLines int = 5
	writer.Write([]byte("\n"))
	lineArray := make([]string, 0)
	for i := 0; i < nLines; i++ {
		lineArray = append(lineArray, fmt.Sprintf("Hello World-%d", i))
	}
	//fmt.Println(lineArray)
	writer.Write([]byte(strings.Join(lineArray, "\n")))
	writer.Flush()
	writer.Close()

	lineChannel := make(chan string)
	commChannel := make(chan struct{})
	go phonelab_backend.ReadFileInReverse(fstruct.File, lineChannel, commChannel)

	var line string
	var ok bool
	var nVerified int = 0
	for {
		line, ok = <-lineChannel
		assert.True(ok, "Should not fail due to closed channel")
		expected := fmt.Sprintf("Hello World-%d", nLines-nVerified-1)
		assert.Equal(expected, line, "Did not match expected string")
		nVerified++
		if nVerified == nLines {
			close(commChannel)
			break
		} else {
			commChannel <- struct{}{}
		}
	}
	fstruct.Close()
	cleanup()
}
*/

/*
func TestAddOutMetadata(t *testing.T) {
	assert := assert.New(t)

	phonelab_backend.StagingDirBase = "/tmp/test-add-metadata/stage"
	phonelab_backend.OutDirBase = "/tmp/test-add-metadata/out"

	port := 31121
	var server *phonelab_backend.Server
	workChannel := make(chan *phonelab_backend.Work, 1000)

	appendMetadata := func(work *phonelab_backend.DeviceWork) {

	}

	phonelab_backend.InitializeProcessingSteps()
	phonelab_backend.PostProcessing = []phonelab_backend.ProcessingFunction{appendMetadata}

	go RunTestServerAsync(port, &server, workChannel)
	UploadFiles(port, 1, 1, assert)
	server.Stop()
}
*/
