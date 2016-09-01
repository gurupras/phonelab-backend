package phonelab_backend

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/gurupras/gocommons"
)

var ()

type DeviceWork struct {
	*Work
	BootId         string
	OutFile        *gocommons.File
	StartTimestamp int64
	EndTimestamp   int64
}

type ProcessingFunction func(work *DeviceWork) (error, bool)

type ProcessingConfig struct {
	PreProcessing  []ProcessingFunction
	Core           func(deviceWork *DeviceWork, processingConfig *ProcessingConfig) error
	PostProcessing []ProcessingFunction
}

func InitializeProcessingConfig() *ProcessingConfig {
	pc := new(ProcessingConfig)

	pc.Core = ProcessStagedWork

	pc.PreProcessing = append(pc.PreProcessing, UpdateOutFile)
	pc.PreProcessing = append(pc.PreProcessing, UpdateMetadata)
	return pc
}

func ProcessStage(functions []ProcessingFunction, work *DeviceWork) (errs []error, fail bool) {
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

func CopyStagedFileToOutput(deviceWork *DeviceWork) (err error, fail bool) {
	var file *os.File
	var n int64

	// Critical
	fail = true

	// Actual processing I guess
	file, err = os.OpenFile(deviceWork.StagingFileName, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to open work.StagingFileName", err)
		return
	}
	var compressedReader *gzip.Reader

	if compressedReader, err = gzip.NewReader(file); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to get gzip.Reader to staged file", err)
		return
	}

	//fmt.Println("Input:", file.Name())
	//fmt.Println("Output:", deviceWork.OutFile.Path)

	//fmt.Println("Processing ...")
	var outWriter gocommons.Writer
	var outFile *gocommons.File

	if outFile, err = gocommons.Open(deviceWork.OutFile.Path, os.O_WRONLY|os.O_APPEND, gocommons.GZ_TRUE); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to open file for writing data", err)
		return
	}
	defer outFile.Close()

	if outWriter, err = outFile.Writer(1048576); err != nil {
		if n, err = io.Copy(&outWriter, compressedReader); err != nil {
			fmt.Fprintln(os.Stderr, "Failed to copy from staging to out", err)
		}
	}
	outWriter.Flush()
	outWriter.Close()

	_ = n
	//fmt.Println("Updated outfile:", n)
	return

}

func ProcessProcessConfig(work *Work, processingConfig *ProcessingConfig) (err error) {
	deviceWork := &DeviceWork{
		Work: work,
	}

	var errs []error
	var fail bool
	//fmt.Println("Starting pre-processing")
	if errs, fail = ProcessStage(processingConfig.PreProcessing, deviceWork); len(errs) > 0 && fail {
		err = errors.New(fmt.Sprintf("Stopping ProcessProcessConfig due to fail condition...\nerrors:\n%v\n", errs))
		return
	}

	if err = processingConfig.Core(deviceWork, processingConfig); err != nil {
		err = errors.New(fmt.Sprintf("Failed core processing: %v", err))
		return
	}

	if errs, fail = ProcessStage(processingConfig.PostProcessing, deviceWork); len(errs) > 0 && fail {
		err = errors.New(fmt.Sprintf("Stopping ProcessProcessConfig due to fail condition...\nerrors:\n%v\n", errs))
		return
	}
	return
}

func ProcessStagedWork(deviceWork *DeviceWork, processingConfig *ProcessingConfig) (err error) {
	// Currently doesn't do anything other than CopyStagedFileToOutput
	err, _ = CopyStagedFileToOutput(deviceWork)
	return
}

func OpenFileAndReader(fpath string) (*gocommons.File, *bufio.Scanner, error) {
	var err error
	var fstruct *gocommons.File
	var reader *bufio.Scanner

	if fstruct, err = gocommons.Open(fpath, os.O_RDONLY, gocommons.GZ_UNKNOWN); err != nil {
		err = errors.New(fmt.Sprintf("Failed to open file: %v", err))
		return nil, nil, err
	}

	if reader, err = fstruct.Reader(1048576); err != nil {
		err = errors.New(fmt.Sprintf("Failed to get reader to file: %v", err))
		fstruct.Close()
		return nil, nil, err
	}
	return fstruct, reader, err
}

func UpdateOutFile(work *DeviceWork) (err error, fail bool) {
	// --------- ASSUMPTION ---------
	// We assume that each log file can only have one boot ID.
	// This is hopefully true
	// ------- END ASSUMPTION -------

	// Critical
	fail = true

	// Find bootID
	var fstruct *gocommons.File
	var reader *bufio.Scanner

	if fstruct, reader, err = OpenFileAndReader(work.StagingFileName); err != nil {
		return
	}
	defer fstruct.Close()

	for reader.Scan() {
		line := reader.Text()
		logline := ParseLogline(line)
		//fmt.Println(line)
		if logline != nil {
			work.BootId = logline.BootId
			break
		}
	}
	//fmt.Println("BootID:", work.BootId)

	var ok bool
	if ok, err = gocommons.Exists(work.Work.OutDir); !ok || err != nil {
		if err = gocommons.Makedirs(work.Work.OutDir); err != nil {
			err = errors.New(fmt.Sprintf("Failed to create output directory: %v", err))
			return
		}
	}

	outfile := filepath.Join(work.Work.OutDir, work.BootId+".gz")
	fmt.Println("outfile:", outfile)
	if work.OutFile, err = gocommons.Open(outfile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, gocommons.GZ_TRUE); err != nil {
		err = errors.New(fmt.Sprintf("Failed to open output file", err))
		return
	}
	//fmt.Println("Assigned outfile:", outfile)
	return
}

func UpdateMetadata(work *DeviceWork) (err error, fail bool) {
	// Critical
	fail = true

	var metadataPath string
	var metadataFile *gocommons.File

	outdir := work.OutDir

	metadataPath = filepath.Join(outdir, work.BootId+".yaml")
	exists := false
	if ok, _ := gocommons.Exists(metadataPath); ok {
		exists = true
	}
	metadataFile, err = gocommons.Open(metadataPath, os.O_CREATE|os.O_RDWR, gocommons.GZ_FALSE)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to update metadata", err)
		return
	}

	outMetadata := &OutMetadata{}
	existingMetadata := &OutMetadata{}

	if exists {
		buf := new(bytes.Buffer)
		if _, err = io.Copy(buf, metadataFile.File); err != nil {
			fmt.Fprintln(os.Stderr, "Failed to read metadata from existing file", err)
			return
		}
		yaml.Unmarshal(buf.Bytes(), existingMetadata)
		// Ensure that the device IDs are the same
		//fmt.Println(outMetadata)
		if strings.Compare(existingMetadata.DeviceId, work.DeviceId) != 0 {
			panic(fmt.Sprintf("DeviceIDs don't match! YAML(%s) != work(%s)", outMetadata.DeviceId, work.DeviceId))
		}
	}
	// We've already verified this if the file existed. So blindly overwrite
	outMetadata.DeviceId = work.DeviceId
	outMetadata.Versions = append(existingMetadata.Versions, work.Version)
	outMetadata.PackageNames = append(existingMetadata.PackageNames, work.PackageName)
	outMetadata.UploadTimestamps = append(existingMetadata.UploadTimestamps, work.UploadTimestamp)
	outMetadata.StartTimestamps = append(existingMetadata.StartTimestamps, work.StartTimestamp)
	outMetadata.EndTimestamps = append(existingMetadata.EndTimestamps, work.EndTimestamp)

	metadataFile.Seek(0, os.SEEK_SET)
	var writer gocommons.Writer
	var bytes []byte
	if writer, err = metadataFile.Writer(0); err != nil {
		err = errors.New(fmt.Sprintf("Failed to get writer for metadata file: %v", err))
		return
	}
	if bytes, err = yaml.Marshal(outMetadata); err != nil {
		err = errors.New(fmt.Sprintf("Failed to convert struct to yaml-bytes: %v", err))
		return
	}
	if _, err = writer.Write(bytes); err != nil {
		err = errors.New(fmt.Sprintf("Failed to write metadata to file: %v", err))
		return
	}
	writer.Flush()
	writer.Close()
	return
}

/*
// FIXME: Cannot random access GZIP streams. As a result, we cannot read
// backwards without reading through all preceeding contents.

func UpdateStartEndTimestamps(work *DeviceWork) error {
	var err error
	var fstruct *gocommons.File
	var reader *bufio.Scanner

	if fstruct, reader, err = OpenFileAndReader(work.StagingFileName); err != nil {
		return err
	}
	defer fstruct.Close()

	for reader.Scan() {
		line := reader.Text()
		logline := ParseLogline(line)
		if logline != nil {
			work.StartTimestamp = logline.Datetime.UnixNano()
			break
		}
	}

	// Now we read the last line to find the last timestamp
	lineChannel := make(chan string)
	commChannel := make(chan struct{})
	go ReadFileInReverse(fstruct.File, lineChannel, commChannel)
	for {
		line := <-lineChannel
		logline := ParseLogline(line)
		if logline != nil {
			work.EndTimestamp = logline.Datetime.UnixNano()
			close(commChannel)
		} else {
			commChannel <- struct{}{}
		}
	}
	fmt.Println("Assigned start/end timestamps")
	return err
}

func ReadFileInReverse(file *os.File, lineChannel chan string, commChannel chan struct{}) error {
	var fileSize int64
	var err error
	var gzipReader *gzip.Reader

	if gzipReader, err = gzip.NewReader(file); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to get gzipReader to file", err)
		return err
	}

	if fileSize, err = file.Seek(0, os.SEEK_END); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to seek to end of file", err)
		return err
	}
	// Keep looking in reverse in 1k chunks to find the first newline character
	currentOffset := fileSize
	buf := new(bytes.Buffer)
	for {
		chunkStart := (currentOffset - 1024)
		if chunkStart < 0 {
			chunkStart = 0
		}

		currentOffset -= 1024
		if currentOffset < 0 {
			currentOffset = 0
		}

		file.Seek(chunkStart, os.SEEK_SET)
		var read int64
		if read, err = io.CopyN(buf, file, 1024); err != nil {
			fmt.Fprintln(os.Stderr, "Failed to read from file", err)
			return err
		}
		_ = read
		chunk := buf.String()
		for {
			if idx := strings.LastIndex(chunk, "\n"); idx != -1 {
				// We found a '\n' and this is the last instance of it in this chunk
				lastLine := chunk[idx+1:]
				lineChannel <- lastLine
				if _, ok := <-commChannel; !ok {
					break
				}
				chunk = chunk[:idx]
			} else {
				break
			}
		}
		if currentOffset < 0 {
			currentOffset = 0
		}
	}
	return err
}
*/
