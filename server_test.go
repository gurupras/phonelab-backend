package phonelab_backend

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/gurupras/gocommons"
	"github.com/labstack/echo"
	"github.com/parnurzeal/gorequest"
	"github.com/stretchr/testify/assert"
)

func postRequest() (gorequest.Response, []error) {
	url := "http://localhost:8082/uploader/version/deviceid/packagename/filename"
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
	var server *Server
	var err error

	result := gocommons.InitResult("TestServerConstructor")

	server, err = New(-1)
	assert.Nil(t, server, "Server was created with a negative port")
	assert.NotNil(t, err, "No error on negative port")

	server, err = New(65536)
	assert.Nil(t, server, "Server was created with a out-of-bounds port")
	assert.NotNil(t, err, "No error on out-of-bounds port")

	server, err = New(14111)
	assert.NotNil(t, server, "Server was not created despite valid port")
	assert.Nil(t, err, "Error on valid port")

	gocommons.HandleResult(t, true, result)
}

func TestSetupServer(t *testing.T) {
	var server *Server
	var err error

	result := gocommons.InitResult("TestSetupServer")

	server, err = SetupServer(-1, false)
	assert.Nil(t, server, "Server was created with a negative port")
	assert.NotNil(t, err, "No error on negative port")

	server, err = SetupServer(14112, false)
	assert.True(t, server != nil, "Server was not created despite valid port")
	assert.True(t, err == nil, "Error on valid port")

	gocommons.HandleResult(t, true, result)
}

func TestRunServer(t *testing.T) {
	result := gocommons.InitResult("TestRunServer")

	var err error

	go func() {
		var server *Server
		server, err = New(8082)
		assert.Nil(t, err, "Failed to start server", err)
		server.POST("/uploader/:version/:deviceId/:packageName/:fileName", testHttpMethod)
		// Start the server
		server.Run()
	}()
	time.Sleep(300 * time.Millisecond)
	resp, errors := postRequest()
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	payload := buf.String()

	assert.True(t, errors == nil, "POST request failed", errors)
	assert.Equal(t, 200, resp.StatusCode, "POST request failed")
	assert.Equal(t, "OK", payload, "POST request failed")

	gocommons.HandleResult(t, true, result)
}
