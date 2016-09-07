package phonelab_backend

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/fatih/set"
	"github.com/gurupras/gocommons"
	"github.com/gurupras/gocommons/seekable_stream"
	"github.com/jehiah/go-strftime"
	"github.com/labstack/echo"
	"github.com/pbnjay/strptime"
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
		err = errors.New(fmt.Sprintf("Could not open tempfile: %v", err))
		return
	}
	defer fstruct.Close()

	// Seek end
	fstruct.Seek(0, os.SEEK_END)

	// We first write all the metadata
	// This cannot fail since we have opened the file with write permissions
	writer, _ = fstruct.Writer(0)

	defer writer.Close()
	defer writer.Flush()

	metadataBuf := new(bytes.Buffer)
	compressedWriter := gzip.NewWriter(metadataBuf)
	metadata := WorkToStagingMetadata(work)
	if err = UpdateStagingMetadataDates(metadata, work); err != nil {
		err = errors.New(fmt.Sprintf("Failed to obtain dates for metadata: %v: %v", work.StagingFileName, err))
		return
	}

	WriteStagingMetadata(compressedWriter, metadata)
	compressedWriter.Flush()
	compressedWriter.Close()

	// Only case in which this can fail is if we somehow run out of disk space
	if n, err = io.Copy(&writer, metadataBuf); err != nil {
		err = errors.New(fmt.Sprintf("Failed to write metadata to %v: %v", work.StagingFileName, err))
		return
	}
	_ = n
	//fmt.Println(fmt.Sprintf("Wrote %d bytes", n))
	return
}

func UpdateStagingMetadataDates(metadata *StagingMetadata, work *Work) (err error) {
	// We need to:
	//   1) Copy entire DataStream into another stream and update work.DataStream
	//   2) Read stream and find set of all dates in use
	//   3) Reset stream
	// Step (1) is needed since io.Reader has no reset. Once we read it, we've consumed it.
	// Therefore, we need to read it, and write it into a buffer and then use this buffer
	// for all other processing
	var reader io.Reader
	var compressedReader *gzip.Reader
	if compressedReader, err = gzip.NewReader(work.DataStream); err != nil {
		//err = errors.New(fmt.Sprintf("Failed to obtain compressed reader to work.DataStream: %v", err))
		err = nil
		reader = work.DataStream
	} else {
		reader = compressedReader
	}

	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)

	set := set.NewNonTS()
	for scanner.Scan() {
		line := scanner.Text()
		logline := ParseLogline(line)
		if logline == nil {
			// Weird...
			continue
		}
		var dt time.Time
		dt, err = strptime.Parse(strftime.Format("%Y-%m-%d", logline.Datetime), "%Y-%m-%d")
		if err != nil {
			err = errors.New(fmt.Sprintf("Failed to get datetime of line: %v: %v", line, err))
			return err
		}
		set.Add(dt)
	}
	for _, data := range set.List() {
		dt := data.(time.Time)
		metadata.Dates = append(metadata.Dates, dt)
	}

	work.DataStream.Rewind()
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
	if file, err = gocommons.TempFile(work.StagingDir, "log-", ".gz"); err != nil {
		err = errors.New(fmt.Sprintf("Failed to create temporary file: %v", err))
		return
	}
	file.Close()

	// Now do the staging part
	work.StagingFileName = file.Name()
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
	} else if len(errs) > 0 {
		fmt.Fprintln(os.Stderr, errs)
	}

	fstruct, err := gocommons.Open(work.StagingFileName, os.O_APPEND|os.O_WRONLY, gocommons.GZ_TRUE)
	if err != nil {
		err = errors.New(fmt.Sprintf("Could not open tempfile: %v: %v", work.StagingFileName, err))
		return
	}

	fstruct.Seek(0, os.SEEK_END)

	// Cannot fail unless the file somehow changed to RDONLY between opening and this statement
	writer, _ := fstruct.Writer(0)

	// Do the payload copy
	// The stream is already compressed
	var compressedInput *gzip.Reader
	var inputReader io.Reader

	if compressedInput, err = gzip.NewReader(work.DataStream); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to get gzip.Reader to input:", err)
		inputReader = work.DataStream
	} else {
		inputReader = compressedInput
	}

	// Cannot fail unless we somehow run out of disk space during the copy
	if bytesWritten, err = io.Copy(&writer, inputReader); err != nil {
		err = errors.New(fmt.Sprintf("Failed to copy input to %v: %v", work.StagingFileName, err))
		return
	}
	// We want to flush/close before post processing. So do that now
	writer.Close()
	fstruct.Close()

	// Now post processing
	if errs, fail = RunStagingProcesses(stagingConfig.PostProcessing, work); len(errs) > 0 && fail {
		err = errors.New(fmt.Sprintf("Stopping HandleUpload due to fail condition...\nerrors:\n%v\n", errs))
		return
	} else if len(errs) > 0 {
		fmt.Fprintln(os.Stderr, errs)
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

	work.DataStream = new(seekable_stream.SeekableStream)
	work.DataStream.WrapReader(body)

	work.StagingDir = filepath.Join(config.StagingDir, deviceId)
	work.OutDir = filepath.Join(config.OutDir, deviceId)

	if _, err = HandleUpload(body, work, config.WorkChannel, config.StagingConfig); err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	} else {
		return c.String(http.StatusOK, "OK")
	}
	//fmt.Printf("Headers:\n%v\n", c.Request().Header())
}
