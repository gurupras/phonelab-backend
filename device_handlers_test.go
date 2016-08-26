package phonelab_backend

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/gurupras/gocommons"
	"github.com/stretchr/testify/assert"
)

func TestProcessStagedWork(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	var port int = 11929
	var server *Server

	var (
		started         int = 0
		verified        int = 0
		nDevices        int = 5
		nFilesPerDevice int = 5
		totalFiles      int = nDevices * nFilesPerDevice
	)

	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}

	preProcess := func(w *DeviceWork) (err error) {
		mutex.Lock()
		started++
		mutex.Unlock()
		return
	}
	postProcess := func(w *DeviceWork) (err error) {
		mutex.Lock()
		verified++
		if verified == totalFiles {
			wg.Done()
		}
		mutex.Unlock()
		return
	}
	customWorkFn := func(w *Work) {
		ProcessStagedWork(w)
	}

	PreProcessing = append(PreProcessing, preProcess)
	PostProcessing = append(PostProcessing, postProcess)

	wg.Add(1)

	workChannel := make(chan *Work, 1000)
	go RunTestServerAsync(port, workChannel, &server, customWorkFn)
	UploadFiles(port, nDevices, nFilesPerDevice, assert)

	close(workChannel)
	wg.Wait()
	server.Stop()
	cleanup()

	assert.Equal(started, verified, fmt.Sprintf("Started(%d) != verified(%d)", started, verified))
}

func TestOpenFileAndReader(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	var fstruct *gocommons.File
	var reader *bufio.Scanner
	var err error

	// Should fail
	fstruct, reader, err = OpenFileAndReader("/deadbeef")
	assert.Nil(fstruct, "No error on non-existent file")
	assert.Nil(reader, "No error on non-existent file")
	assert.NotNil(err, "No error on non-existent file")

	// Fail to read write-only file
	f, err := ioutil.TempFile("/tmp", "wronly-")
	assert.Nil(err, "Failed to create temporary write-only file")
	f.Chmod(0222)
	fstruct, reader, err = OpenFileAndReader(f.Name())
	assert.Nil(fstruct, "No error on write-only file")
	assert.Nil(reader, "No error on write-only file")
	assert.NotNil(err, "No error on write-only file")

	// Now succeed on proper file
	f.Chmod(0666)
	fstruct, reader, err = OpenFileAndReader(f.Name())
	assert.NotNil(fstruct, "Error on proper file")
	assert.NotNil(reader, "Error on proper file")
	assert.Nil(err, "Error on proper file")
	fstruct.Close()

	f.Close()
	os.Remove(f.Name())
}

func TestReadFileInReverse(t *testing.T) {
	assert := assert.New(t)

	port := 21331

	workChannel := make(chan *Work)
	var server *Server
	var work *Work
	wg := sync.WaitGroup{}

	workFn := func(w *Work) {
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
	fstruct, err := gocommons.Open(fpath, os.O_RDWR, gocommons.GZ_UNKNOWN)
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
	go ReadFileInReverse(fstruct.File, lineChannel, commChannel)

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
