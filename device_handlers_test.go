package phonelab_backend_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gurupras/phonelab_backend"
	"github.com/labstack/gommon/log"
	"github.com/stretchr/testify/assert"
)

func TestProcessStagedWork(t *testing.T) {
	//t.Parallel()
	assert := assert.New(t)

	var err error

	defer Recover("TestProcessStagedWork", assert)

	processingConfig := new(phonelab_backend.ProcessingConfig)
	processingConfig.WorkSetCheckPeriod = 1 * time.Second
	processingConfig.DelayBeforeProcessing = 3 * time.Second

	dummyCore := func(work *phonelab_backend.DeviceWork, processingConfig *phonelab_backend.ProcessingConfig) (err error) {
		return
	}

	dummyCoreThrowsError := func(work *phonelab_backend.DeviceWork, processingConfig *phonelab_backend.ProcessingConfig) (err error) {
		return errors.New("Expected")
	}

	// Test for error handling
	errFn := func(work *phonelab_backend.DeviceWork) (error, bool) {
		// Just throw an error
		logger.Debug("Throwing error")
		return errors.New("Expected"), true
	}

	dummyWorks := []*phonelab_backend.Work{&phonelab_backend.Work{}}
	processingConfig.PreProcessing = append(processingConfig.PreProcessing, errFn)
	processingConfig.Core = dummyCore
	err = phonelab_backend.ProcessProcessConfig(dummyWorks, processingConfig)
	assert.NotNil(err, "Pre-processing error not properly handled")
	processingConfig.PreProcessing = processingConfig.PreProcessing[:0]

	// Now test core-processing error handling
	//phonelab_backend.PostProcessing = append(phonelab_backend.PostProcessing, errFn)
	processingConfig.Core = dummyCoreThrowsError
	err = phonelab_backend.ProcessProcessConfig(dummyWorks, processingConfig)
	assert.NotNil(err, "Core-processing error not properly handled")
	processingConfig.Core = dummyCore

	// Now test post-processing error handling
	// We create a dummy core process and feed this into ProcessStagedWork
	processingConfig.PostProcessing = append(processingConfig.PostProcessing, errFn)
	processingConfig.Core = dummyCore
	err = phonelab_backend.ProcessProcessConfig(dummyWorks, processingConfig)
	assert.NotNil(err, "Post-processing error not properly handled")
	processingConfig.PostProcessing = processingConfig.PostProcessing[:0]

	// Now, test for success
	processingConfig = nil

	var port int = 11929
	var server *phonelab_backend.Server

	var (
		started         int = 0
		verified        int = 0
		nDevices        int = 5
		nFilesPerDevice int = 5
		totalFiles      int = nDevices * nFilesPerDevice
	)

	config := new(phonelab_backend.Config)
	config.StagingConfig = phonelab_backend.InitializeStagingConfig()
	config.ProcessingConfig = phonelab_backend.InitializeProcessingConfig()
	config.ProcessingConfig.WorkSetCheckPeriod = 1 * time.Second
	config.ProcessingConfig.DelayBeforeProcessing = 3 * time.Second

	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}

	preProcess := func(w *phonelab_backend.DeviceWork) (err error, fail bool) {
		log.Info("preProcess")
		mutex.Lock()
		started += len(w.WorkList)
		mutex.Unlock()
		return
	}
	postProcess := func(w *phonelab_backend.DeviceWork) (err error, fail bool) {
		mutex.Lock()
		verified += len(w.WorkList)
		log.Info("Verified:", verified)
		if verified == totalFiles {
			wg.Done()
		}
		mutex.Unlock()
		return
	}
	customCore := func(w *phonelab_backend.DeviceWork, processingConfig *phonelab_backend.ProcessingConfig) error {
		return phonelab_backend.ProcessStagedWork(w, processingConfig)
	}
	config.ProcessingConfig.Core = customCore

	config.ProcessingConfig.PreProcessing = append(config.ProcessingConfig.PreProcessing, preProcess)
	config.ProcessingConfig.PostProcessing = append(config.ProcessingConfig.PostProcessing, postProcess)

	wg.Add(1)

	config.WorkChannel = make(chan *phonelab_backend.Work, 1000)
	serverWg := sync.WaitGroup{}
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		RunTestServerAsync(port, config, &server)
	}()
	UploadFiles(port, nDevices, nFilesPerDevice, assert)

	wg.Wait()
	server.Stop()
	serverWg.Wait()
	cleanup()

	assert.Equal(started, verified, fmt.Sprintf("Started(%d) != verified(%d)", started, verified))
}
