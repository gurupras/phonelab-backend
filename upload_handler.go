package phonelab_backend

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"

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
	path := file.Name()
	file.Close()

	// Now do the staging part

	// The stream is already compressed
	fstruct, err := gocommons.Open(path, os.O_WRONLY, gocommons.GZ_FALSE)
	if err != nil {
		panic("Could not open tempfile")
	}

	// We first write all the metadata
	writer, err := fstruct.Writer(0)
	if err != nil {
		panic("Could not get writer to tempfile")
	}

	WriteStagingMetadata(&writer, work)

	if bytesWritten, err = io.Copy(&writer, input); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to copy input to:", file.Name)
		return
	}

	//fmt.Println(fmt.Sprintf("Wrote %v bytes to :%v", bytesWritten, file.Name()))

	work.StagingFileName = file.Name()
	workChannel <- work

	return
}

func HandleUploaderPost(c echo.Context) (err error) {
	version := c.P(0)
	deviceId := c.P(1)
	packageName := c.P(2)
	fileName := c.P(3)

	work := &Work{
		Version:         version,
		DeviceId:        deviceId,
		PackageName:     packageName,
		LogFileName:     fileName,
		UploadTimestamp: time.Now().UnixNano(),
		StagingFileName: "", // This is filled in by HandleUpload
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
