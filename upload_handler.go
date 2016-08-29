package phonelab_backend

import (
	"bytes"
	"compress/gzip"
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

var (
	StagingPreProcessing  []StagingProcess
	StagingPostProcessing []StagingProcess
)

type StagingProcess func(work *Work) error

func InitializeProcessingSteps() {
	InitializeStagingProcessingSteps()
	InitializeDeviceProcessingSteps()
}

func InitializeStagingProcessingSteps() {
	StagingPreProcessing = append(StagingPreProcessing, UpdateStagingMetadata)
}

func UpdateStagingMetadata(work *Work) (err error) {
	var fstruct *gocommons.File
	var writer gocommons.Writer
	var n int64

	// The stream is already compressed
	if fstruct, err = gocommons.Open(work.StagingFileName, os.O_APPEND|os.O_WRONLY, gocommons.GZ_FALSE); err != nil {
		panic("Could not open tempfile")
	}
	defer fstruct.Close()

	// Seek end
	fstruct.Seek(0, os.SEEK_END)

	// We first write all the metadata
	if writer, err = fstruct.Writer(0); err != nil {
		panic("Could not get writer to tempfile")
	}
	defer writer.Close()
	defer writer.Flush()

	metadataBuf := new(bytes.Buffer)
	compressedWriter := gzip.NewWriter(metadataBuf)
	WriteStagingMetadata(compressedWriter, work)
	compressedWriter.Flush()
	compressedWriter.Close()

	if n, err = io.Copy(&writer, metadataBuf); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to write metadata to:", work.StagingFileName)
		return
	}
	_ = n
	//fmt.Println(fmt.Sprintf("Wrote %d bytes", n))
	return
}

func HandleUpload(input io.Reader, work *Work, workChannel chan *Work) (bytesWritten int64, err error) {
	var file *os.File

	gocommons.Makedirs(work.StagingDir)
	if file, err = ioutil.TempFile(work.StagingDir, "log-"); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to create temporary file", err)
		return
	}
	file.Close()
	os.Remove(file.Name())

	path := file.Name() + ".gz"
	if file, err = os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to create temporary file with .gz extension", err)
		return
	}
	file.Close()

	// Now do the staging part
	work.StagingFileName = path

	// Pre processing
	for _, process := range StagingPreProcessing {
		if err = process(work); err != nil {
			fmt.Fprintln(os.Stderr, "Failed pre-processing step", err)
			return
		}
	}
	// The stream is already compressed
	fstruct, err := gocommons.Open(path, os.O_APPEND|os.O_WRONLY, gocommons.GZ_TRUE)
	if err != nil {
		panic("Could not open tempfile")
	}

	fstruct.Seek(0, os.SEEK_END)

	writer, err := fstruct.Writer(0)
	if err != nil {
		panic("Could not get writer to tempfile")
	}

	// Do the payload copy
	var compressedInput *gzip.Reader
	if compressedInput, err = gzip.NewReader(input); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to get gzip.Reader to input:", err)
		return
	}
	if bytesWritten, err = io.Copy(&writer, compressedInput); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to copy input to:", path, err)
		return
	}
	writer.Close()
	fstruct.Close()

	// Post processing
	for _, process := range StagingPostProcessing {
		if err = process(work); err != nil {
			fmt.Fprintln(os.Stderr, "Failed post-processing step", err)
			return
		}
	}

	// TODO: Move this to post-processing
	err = os.Chmod(path, 0440)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("Failed to make staging "+
			"file read-only:\n%s\n"+
			"This is not a fatal error..continuing", err))
	}
	//fmt.Println(fmt.Sprintf("Wrote %v bytes to :%v", bytesWritten, file.Name()))

	// We want to flush/close before passing on the work to the device. So do that now
	workChannel <- work

	return
}

func HandleUploaderPost(c echo.Context, config *Config) (err error) {
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

	work.StagingDir = filepath.Join(config.StagingDir, deviceId)
	work.OutDir = filepath.Join(config.OutDir, deviceId)

	HandleUpload(body, work, config.WorkChannel)

	// Currently unused
	_ = body
	_ = version
	_ = packageName
	_ = fileName

	//fmt.Printf("Headers:\n%v\n", c.Request().Header())

	return c.String(http.StatusOK, "OK")
}
