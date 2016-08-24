package phonelab_backend

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gurupras/gocommons"
	"github.com/labstack/echo"
)

func HandleUpload(input io.Reader, work *Work, stagingDir string, workChannel chan *Work) (bytesWritten int64, err error) {
	var file *os.File

	gocommons.Makedirs(stagingDir)
	if file, err = ioutil.TempFile(stagingDir, "log-"); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to create temporary file", err)
		return
	}
	defer file.Close()
	if bytesWritten, err = io.Copy(file, input); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to copy input to:", file.Name)
		return
	}

	//fmt.Println(fmt.Sprintf("Wrote %v bytes to :%v", bytesWritten, file.Name()))

	work.FilePath = file.Name()
	workChannel <- work

	return
}

func HandleUploaderPost(c echo.Context) (err error) {
	version := c.P(0)
	deviceId := c.P(1)
	packageName := c.P(2)
	fileName := c.P(3)

	work := &Work{
		FilePath:    "", // This is filled in by HandleUpload
		Version:     version,
		DeviceId:    deviceId,
		PackageName: packageName,
		FileName:    fileName,
	}

	// The body is a compressed stream represented by io.Reader
	body := c.Request().Body()

	stagingDir := filepath.Join(StagingDirBase, deviceId)
	outDirBase := filepath.Join(OutDirBase, deviceId)

	HandleUpload(body, work, stagingDir, PendingWorkChannel)

	// Currently unused
	_ = body
	_ = stagingDir
	_ = version
	_ = packageName
	_ = fileName
	_ = outDirBase

	//fmt.Printf("Headers:\n%v\n", c.Request().Header())

	return c.String(http.StatusOK, "OK")
}

func HandleUploaderGet(c echo.Context) (err error) {
	version := c.P(0)
	deviceId := c.P(1)
	packageName := c.P(2)
	fileName := c.P(3)

	stagingDir := filepath.Join(StagingDirBase, deviceId)
	outDirBase := filepath.Join(OutDirBase, deviceId)

	// Currently unused
	_ = version
	_ = packageName
	_ = fileName
	_ = outDirBase
	_ = stagingDir

	return c.String(http.StatusOK, "OK")
}
