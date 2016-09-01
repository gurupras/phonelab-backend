package phonelab_backend

import (
	"bytes"
	"compress/gzip"
	"errors"
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

type StagingProcess func(work *Work) (error, bool)

type StagingConfig struct {
	PreProcessing  []StagingProcess
	PostProcessing []StagingProcess
}

func InitializeStagingConfig() *StagingConfig {
	sc := new(StagingConfig)
	sc.PreProcessing = append(sc.PreProcessing, CreateStagingFile)
	sc.PreProcessing = append(sc.PreProcessing, UpdateStagingMetadata)

	sc.PostProcessing = append(sc.PostProcessing, MakeStagedFileReadOnly)
	return sc
}

func UpdateStagingMetadata(work *Work) (err error, fail bool) {
	var fstruct *gocommons.File
	var writer gocommons.Writer
	var n int64

	// Updating metadata is mandatory
	fail = true

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

func RunStagingProcesses(functions []StagingProcess, work *Work) (errs []error, fail bool) {
	var err error
	for _, fn := range functions {
		if err, fail = fn(work); err != nil {
			err = errors.New(fmt.Sprintf("Failed to run processing stage: %v", err))
			errs = append(errs, err)
			if fail {
				return
			}
		}
	}
	return
}

func CreateStagingFile(work *Work) (err error, fail bool) {
	// Mandatory step
	fail = true

	var file *os.File

	gocommons.Makedirs(work.StagingDir)
	if file, err = ioutil.TempFile(work.StagingDir, "log-"); err != nil {
		err = errors.New(fmt.Sprintf("Failed to create temporary file: %v", err))
		return
	}
	file.Close()
	os.Remove(file.Name())

	path := file.Name() + ".gz"
	if file, err = os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644); err != nil {
		err = errors.New(fmt.Sprintf("Failed to create temporary file with .gz extension: %v", err))
		return
	}
	file.Close()

	// Now do the staging part
	work.StagingFileName = path
	return
}

func MakeStagedFileReadOnly(work *Work) (err error, fail bool) {
	// Not a mandatory step
	fail = false

	err = os.Chmod(work.StagingFileName, 0440)
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to make staging file read-only: %v", err))
		return
	}
	return
}

func HandleUpload(input io.Reader, work *Work, workChannel chan *Work, stagingConfig *StagingConfig) (bytesWritten int64, err error) {
	var fail bool
	var errs []error
	if errs, fail = RunStagingProcesses(stagingConfig.PreProcessing, work); len(errs) > 0 && fail {
		err = errors.New(fmt.Sprintf("Stopping HandleUpload due to fail condition...\nerrors:\n%v\n", errs))
		return
	}

	fstruct, err := gocommons.Open(work.StagingFileName, os.O_APPEND|os.O_WRONLY, gocommons.GZ_TRUE)
	if err != nil {
		panic("Could not open tempfile")
	}

	fstruct.Seek(0, os.SEEK_END)

	writer, err := fstruct.Writer(0)
	if err != nil {
		panic("Could not get writer to tempfile")
	}

	// Do the payload copy
	// The stream is already compressed
	var compressedInput *gzip.Reader
	var inputReader io.Reader

	if compressedInput, err = gzip.NewReader(input); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to get gzip.Reader to input:", err)
		inputReader = input
	} else {
		inputReader = compressedInput
	}
	if bytesWritten, err = io.Copy(&writer, inputReader); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to copy input to:", work.StagingFileName, err)
		return
	}
	// We want to flush/close before post processing. So do that now
	writer.Close()
	fstruct.Close()

	// Now post processing
	if errs, fail = RunStagingProcesses(stagingConfig.PostProcessing, work); len(errs) > 0 && fail {
		err = errors.New(fmt.Sprintf("Stopping HandleUpload due to fail condition...\nerrors:\n%v\n", errs))
		return
	}

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

	if _, err = HandleUpload(body, work, config.WorkChannel, config.StagingConfig); err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	} else {
		return c.String(http.StatusOK, "OK")
	}
	//fmt.Printf("Headers:\n%v\n", c.Request().Header())
}
