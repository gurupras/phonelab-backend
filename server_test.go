package phonelab_backend_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

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

func TestServerConstructor(t *testing.T) {
	t.Parallel()
	var server *phonelab_backend.Server
	var err error
	assert := assert.New(t)

	defer Recover("TestServerConstructor")

	server, err = phonelab_backend.New(-1)
	assert.Nil(server, "Server was created with a negative port")
	assert.NotNil(err, "No error on negative port")

	server, err = phonelab_backend.New(65536)
	assert.Nil(server, "Server was created with a out-of-bounds port")
	assert.NotNil(err, "No error on out-of-bounds port")

	server, err = phonelab_backend.New(14111)
	assert.NotNil(server, "Server was not created despite valid port")
	assert.Nil(err, "Error on valid port")
}

func TestSetupServer(t *testing.T) {
	t.Parallel()
	var server *phonelab_backend.Server
	var err error

	assert := assert.New(t)

	defer Recover("TestSetupServer")

	config := new(phonelab_backend.Config)

	server, err = phonelab_backend.SetupServer(-1, config, false)
	assert.Nil(server, "Server was created with a negative port")
	assert.NotNil(err, "No error on negative port")

	server, err = phonelab_backend.SetupServer(14112, config, false)
	assert.True(server != nil, "Server was not created despite valid port")
	assert.True(err == nil, "Error on valid port")
	server.Stop()

	// Test server without logger for converage
	server, err = phonelab_backend.SetupServer(14112, config, true)
	assert.True(server != nil, "Server was not created despite valid port")
	assert.True(err == nil, "Error on valid port")
	server.Stop()
}

func TestRunServer(t *testing.T) {
	t.Parallel()
	var err error

	var port int = 8082

	assert := assert.New(t)

	defer Recover("TestRunServer")

	go func() {
		var server *phonelab_backend.Server
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
}
