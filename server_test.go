package phonelab_backend_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gurupras/phonelab_backend"
	"github.com/labstack/echo"
	"github.com/parnurzeal/gorequest"
	"github.com/stretchr/testify/assert"
)

func postRequest(port int) (gorequest.Response, []error) {
	url := fmt.Sprintf("http://localhost:%d/uploader/version/deviceid/packagename/filename", port)
	payload := "hello world"
	resp, _, err := gorequest.New().Post(url).
		Set("Content-Length", fmt.Sprintf("%v", len(payload))).
		Set("Accept-Encoding", "gzip").
		Set("Content-Type", "application/x-www-form-urlencoded").
		Send(payload).
		End()
	return resp, err
}

func testHttpMethod(c echo.Context) (err error) {
	return c.String(200, "OK")
}

type Hook struct {
	Callback func(entry *logrus.Entry) (err error)
}

func (h Hook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h Hook) Fire(entry *logrus.Entry) (err error) {
	return h.Callback(entry)
}

func TestInitLogger(t *testing.T) {
	//t.Parallel()

	assert := assert.New(t)

	wg := sync.WaitGroup{}

	var message string
	var testLogger *logrus.Logger

	hook := Hook{}
	hook.Callback = func(entry *logrus.Entry) (err error) {
		defer wg.Done()
		assert.Equal(message, entry.Message, "Messages don't match")
		return nil
	}

	// Negative tests
	// test nil? size loggers
	var nilLoggers []*logrus.Logger
	phonelab_backend.InitLogger(nilLoggers...)
	testLogger = phonelab_backend.GetLogger()
	message = "Testing InitLogger() with nil loggers"
	wg.Add(1)
	testLogger.Hooks.Add(hook)
	testLogger.Debugln(message)
	wg.Wait()
	testLogger.Hooks = make(logrus.LevelHooks)

	// len 0
	loggers := []*logrus.Logger{}
	phonelab_backend.InitLogger(loggers...)
	testLogger = phonelab_backend.GetLogger()
	message = "Testing InitLogger() with len 0"
	wg.Add(1)
	testLogger.Hooks.Add(hook)
	testLogger.Debugln(message)
	wg.Wait()
	testLogger.Hooks = make(logrus.LevelHooks)

	// Now test custom testLogger
	testLogger = logrus.New()
	testLogger.Level = logrus.DebugLevel
	message = "Testing InitLogger() with custom testLogger"
	wg.Add(1)
	testLogger.Hooks.Add(hook)
	phonelab_backend.InitLogger([]*logrus.Logger{testLogger}...)
	serverLogger := phonelab_backend.GetLogger()
	assert.Equal(testLogger, serverLogger, "Loggers don't match")
	testLogger.Debugln(message)
	wg.Wait()
	testLogger.Hooks = make(logrus.LevelHooks)
}

func TestServerConstructor(t *testing.T) {
	//t.Parallel()
	var server *phonelab_backend.Server
	var err error
	assert := assert.New(t)

	defer Recover("TestServerConstructor", assert)

	server, err = phonelab_backend.New(-1)
	assert.Nil(server, "Server was created with a negative port")
	assert.NotNil(err, "No error on negative port")

	server, err = phonelab_backend.New(65536)
	assert.Nil(server, "Server was created with a out-of-bounds port")
	assert.NotNil(err, "No error on out-of-bounds port")

	server, err = phonelab_backend.New(14111)
	assert.NotNil(server, "Server was not created despite valid port")
	assert.Nil(err, "Error on valid port")
	logger.Debug("Attempting to stop server")
}

func TestSetupServer(t *testing.T) {
	//t.Parallel()
	var server *phonelab_backend.Server
	var err error

	assert := assert.New(t)

	defer Recover("TestSetupServer", assert)

	config := new(phonelab_backend.Config)

	server, err = phonelab_backend.SetupServer(-1, config, false)
	assert.Nil(server, "Server was created with a negative port")
	assert.NotNil(err, "No error on negative port")

	server, err = phonelab_backend.SetupServer(14112, config, false)
	assert.True(server != nil, "Server was not created despite valid port")
	assert.True(err == nil, "Error on valid port")

	// Test with initialized config.WorkChannel
	config.WorkChannel = make(chan *phonelab_backend.Work)
	server, err = phonelab_backend.SetupServer(14113, config, true)
	assert.True(server != nil, "Server was not created despite valid port")
	assert.True(err == nil, "Error on valid port")
	config.WorkChannel = nil

	// Test server without logger for converage
	server, err = phonelab_backend.SetupServer(14114, config, true)
	assert.True(server != nil, "Server was not created despite valid port")
	assert.True(err == nil, "Error on valid port")
}

func TestRunServer(t *testing.T) {
	//t.Parallel()
	var err error

	var port int = 8082

	assert := assert.New(t)

	defer Recover("TestRunServer", assert)

	var server *phonelab_backend.Server
	go func() {
		server, err = phonelab_backend.New(port)
		assert.Nil(err, "Failed to start server", err)
		server.POST("/uploader/:version/:deviceId/:packageName/:fileName", testHttpMethod)
		// Start the server
		server.Run()
	}()
	time.Sleep(300 * time.Millisecond)
	resp, errors := postRequest(port)
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	payload := buf.String()

	assert.True(errors == nil, "POST request failed", errors)
	assert.Equal(200, resp.StatusCode, "POST request failed")
	assert.Equal("OK", payload, "POST request failed")
	server.Stop()
}
