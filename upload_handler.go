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
	"strings"
	"time"

	"github.com/fatih/set"
	"github.com/gurupras/gocommons"
	"github.com/gurupras/gocommons/seekable_stream"
	"github.com/jehiah/go-strftime"
	"github.com/labstack/echo"
	"github.com/pbnjay/strptime"
)

type Work struct {
	StagingMetadata
	StagingFileName string
	StagingDir      string
	OutDir          string
	DataStream      *seekable_stream.SeekableStream
}

type UploadMetadata struct {
	Version         string `yaml:version`
	DeviceId        string `yaml:device_id`
	PackageName     string `yaml:package_name`
	UploadTimestamp int64  `yaml:upload_timestamp`
	UploadFileName  string `yaml:upload_filename`
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
	if err = UpdateStagingMetadataFields(metadata, work); err != nil {
		err = errors.New(fmt.Sprintf("Failed to obtain dates for metadata: %v: %v", work.StagingFileName, err))
		return
	}

	WriteMetadata(compressedWriter, metadata)
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

func UpdateStagingMetadataFields(metadata *StagingMetadata, work *Work) (err error) {
	// We need to:
	//   1) Copy entire DataStream into another stream and update work.DataStream
	//   2) Read stream and find set of all dates in use
	//   3) Reset stream
	// Step (1) is needed since io.Reader has no reset. Once we read it, we've consumed it.
	// Therefore, we need to read it, and write it into a buffer and then use this buffer
	// for all other processing
	var compressedReader *gzip.Reader
	if compressedReader, err = gzip.NewReader(work.DataStream); err != nil {
		err = errors.New(fmt.Sprintf("Failed to obtain compressed reader to work.DataStream: %v", err))
		return
	}

	scanner := bufio.NewScanner(compressedReader)
	scanner.Split(bufio.ScanLines)

	dates := set.NewNonTS()
	var logline *Logline

	tags := set.NewNonTS()
	bootIds := set.NewNonTS()

	for scanner.Scan() {
		line := scanner.Text()
		// Sometimes, we get weird lines
		logline, _ = ParseLogline(line)

		var dt time.Time
		if dt, err = strptime.Parse(strftime.Format("%Y-%m-%d", logline.Datetime), "%Y-%m-%d"); err != nil {
			err = errors.New(fmt.Sprintf("Failed to get datetime of line: %v: %v", line, err))
			return err
		}
		dates.Add(dt)

		tags.Add(logline.Tag)
		bootIds.Add(logline.BootId)
	}

	for _, data := range dates.List() {
		dt := data.(time.Time)
		metadata.Dates = append(metadata.Dates, dt)
	}
	for _, data := range tags.List() {
		tag := data.(string)
		metadata.Tags = append(metadata.Tags, tag)
	}
	for _, data := range bootIds.List() {
		bootId := data.(string)
		metadata.BootIds = append(metadata.BootIds, bootId)
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

// Gets the name of a new temp file..but the file does not exist
func GetTempFile(dir, prefix string, suffix ...string) (string, error) {
	var err error
	var file *os.File
	if err = gocommons.Makedirs(dir); err != nil {
		err = errors.New(fmt.Sprintf("Failed to create dir for GetTempFile: %v", err))
		return "", err
	}
	if file, err = gocommons.TempFile(dir, prefix, suffix...); err != nil {
		err = errors.New(fmt.Sprintf("Failed to create temporary file: %v", err))
		return "", err
	}

	file.Close()
	os.Remove(file.Name())
	return file.Name(), err
}

func CreateStagingFile(work *Work) (err error, fail bool) {
	// Mandatory step
	fail = true

	var file *os.File
	var fileName string

	if fileName, err = GetTempFile(work.StagingDir, "log-", ".gz"); err != nil {
		err = errors.New(fmt.Sprintf("Failed CreateStagingFile: %v", err))
		return
	}

	if file, err = os.OpenFile(fileName, os.O_CREATE|os.O_TRUNC|os.O_RDONLY, 0664); err != nil {
		err = errors.New(fmt.Sprintf("Failed CreateStagingFile: %v", err))
		return
	}

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
	var fail bool = true
	var errs []error

	if work == nil {
		err = errors.New("Work is nil")
		return
	}
	// Update work.StagingFileName
	if work.StagingFileName, err = GetTempFile(work.StagingDir, "log-", ".gz"); err != nil {
		err = errors.New(fmt.Sprintf("Failed to update work.StagingFileName: %v", err))
		return
	}

	// Pre-processing
	if errs, fail = RunStagingProcesses(stagingConfig.PreProcessing, work); len(errs) > 0 && fail {
		err = errors.New(fmt.Sprintf("Stopping HandleUpload due to fail condition...\nerrors:\n%v\n", errs))
		return
	} else if len(errs) > 0 {
		fmt.Fprintln(os.Stderr, errs)
	}

	// Update staging metadata fields
	if err = UpdateStagingMetadataFields(&work.StagingMetadata, work); err != nil {
		err = errors.New(fmt.Sprintf("Failed UpdateStagingMetadataFields: %v", err))
		return
	}
	// We still haven't written the metadata to the file. This will be
	// done for each chunk

	// Split the file into chunks depending on number of dates present
	originalWork := work
	// Remove extension
	basename := filepath.Base(work.StagingFileName)
	ext := filepath.Ext(basename)
	fileName := filepath.Join(work.StagingDir, basename[:len(basename)-len(ext)])

	chunkMap := make(map[time.Time]*Work)

	//fmt.Println("Dates:", work.StagingMetadata.Dates)
	// Split
	for idx, date := range work.StagingMetadata.Dates {
		var chunkName string
		var chunkWork *Work = new(Work)
		*chunkWork = *originalWork
		chunkWork.StagingMetadata.Dates = []time.Time{date}

		if idx > 1 {
			chunkName = fmt.Sprintf("%v-%v", fileName, idx)
		} else {
			chunkName = fileName
		}

		// Add back the .gz
		chunkName += ".gz"

		chunkWork.StagingFileName = chunkName
		// Create the file
		var file *os.File
		if file, err = os.OpenFile(chunkName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0664); err != nil {
			err = errors.New(fmt.Sprintf("Failed to create chunk file: %v", err))
			return
		}
		file.Close()

		// Now that the file is created, update its metadata
		if err, fail = UpdateStagingMetadata(chunkWork); err != nil {
			err = errors.New(fmt.Sprintf("Failed to update staging metadata for chunk: %v", err))
			if fail {
				return
			}
		}
		chunkMap[date] = chunkWork
	}

	//First, open all chunk files and maintain the pointers so we can just
	// write to the corresponding files
	dateChunkWriterMap := make(map[time.Time]gocommons.Writer)
	for date, chunkWork := range chunkMap {
		//fmt.Println(fmt.Sprintf("%v -> %v", chunkWork.StagingFileName, date))
		var fstruct *gocommons.File
		fstruct, err = gocommons.Open(chunkWork.StagingFileName, os.O_APPEND|os.O_WRONLY, gocommons.GZ_TRUE)
		if err != nil {
			err = errors.New(fmt.Sprintf("Could not open tempfile: %v: %v", chunkWork.StagingFileName, err))
			return
		}

		fstruct.Seek(0, os.SEEK_END)

		// Cannot fail unless the file somehow changed to RDONLY between opening and this statement
		writer, _ := fstruct.Writer(0)

		dateChunkWriterMap[date] = writer
	}

	// Now, start reading the input stream and writing to chunks
	var compressedInput *gzip.Reader
	var inputReader io.Reader

	if compressedInput, err = gzip.NewReader(work.DataStream); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to get gzip.Reader to input:", err)
		inputReader = work.DataStream
	} else {
		inputReader = compressedInput
	}

	scanner := bufio.NewScanner(inputReader)
	scanner.Split(bufio.ScanLines)

	// Now, write line by line
	var logline *Logline
	dataMap := make(map[time.Time][]string)
	for date, _ := range chunkMap {
		dataMap[date] = make([]string, 0)
	}
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)

		if logline, err = ParseLogline(line); err != nil {
			err = errors.New(fmt.Sprintf("Failed to parse logline while writing to chunk: %v", err))
			return
		}

		dateStr := strftime.Format("%Y-%m-%d", logline.Datetime)
		date := strptime.MustParse(dateStr, "%Y-%m-%d")

		if _, ok := dataMap[date]; !ok {
			err = errors.New(fmt.Sprintf("date:%v does not have an entry in dataMap", date))
			return
		}
		dataMap[date] = append(dataMap[date], line)
	}
	for date, writer := range dateChunkWriterMap {
		// Find writer based on this date
		data := dataMap[date]
		if len(data) == 0 {
			// Nothing to write here?
			continue
		}
		writer.Write([]byte(strings.Join(dataMap[date], "\n")))
		// We want to flush/close before post processing. So do that now
		writer.Flush()
		writer.Close()
	}

	// Now post processing
	// Run for each chunk
	for _, chunkWork := range chunkMap {
		if errs, fail = RunStagingProcesses(stagingConfig.PostProcessing, chunkWork); len(errs) > 0 && fail {
			err = errors.New(fmt.Sprintf("Stopping HandleUpload due to fail condition...\nerrors:\n%v\n", errs))
			return
		} else if len(errs) > 0 {
			fmt.Fprintln(os.Stderr, errs)
		}
	}

	// Write all chunks to workChannel
	for _, chunkWork := range chunkMap {
		workChannel <- chunkWork
	}
	return
}

func HandleUploaderPost(c echo.Context, config *Config) (err error) {
	version := c.P(0)
	deviceId := c.P(1)
	packageName := c.P(2)
	fileName := c.P(3)

	work := &Work{}
	work.UploadFileName = fileName
	work.Version = version
	work.DeviceId = deviceId
	work.PackageName = packageName
	work.UploadTimestamp = time.Now().UnixNano()
	work.StagingFileName = "" // This is filled in by HandleUpload

	// The body is a compressed stream represented by io.Reader
	body := c.Request().Body()

	work.DataStream = new(seekable_stream.SeekableStream)
	work.DataStream.WrapReader(body)

	// Test stream for compression.
	if _, err = gzip.NewReader(work.DataStream); err != nil {
		err = errors.New(fmt.Sprintf("POST data is not compressed: %v", err))
		return c.String(http.StatusInternalServerError, err.Error())
	}

	// Rewind the stream
	work.DataStream.Rewind()

	work.StagingDir = filepath.Join(config.StagingDir, deviceId)
	work.OutDir = config.OutDir

	if _, err = HandleUpload(body, work, config.WorkChannel, config.StagingConfig); err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	} else {
		return c.String(http.StatusOK, "OK")
	}
	//fmt.Printf("Headers:\n%v\n", c.Request().Header())
}
