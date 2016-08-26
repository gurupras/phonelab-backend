package phonelab_backend

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/gurupras/gocommons"
)

var (
	PreProcessing  []ProcessingFunction
	PostProcessing []ProcessingFunction
)

type DeviceWork struct {
	*Work
	BootId         string
	OutFile        *gocommons.File
	OutWriter      gocommons.Writer
	StartTimestamp int64
	EndTimestamp   int64
}

type ProcessingFunction func(work *DeviceWork) error

func ProcessStagedWork(work *Work) {
	deviceWork := &DeviceWork{
		Work: work,
	}
	for _, process := range PreProcessing {
		process(deviceWork)
	}
	// Actual processing I guess
	for _, process := range PostProcessing {
		process(deviceWork)
	}
}

func OpenFileAndReader(fpath string) (*gocommons.File, *bufio.Scanner, error) {
	var err error
	var fstruct *gocommons.File
	var reader *bufio.Scanner

	if fstruct, err = gocommons.Open(fpath, os.O_RDONLY, gocommons.GZ_UNKNOWN); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to open file", err)
		return nil, nil, err
	}

	if reader, err = fstruct.Reader(1048576); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to get reader to file", err)
		fstruct.Close()
		return nil, nil, err
	}
	return fstruct, reader, err
}

func UpdateOutFile(work *DeviceWork) error {
	// --------- ASSUMPTION ---------
	// We assume that each log file can only have one boot ID.
	// This is hopefully true
	// ------- END ASSUMPTION -------

	// Find bootID
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
			work.BootId = logline.BootId
			break
		}
	}

	outdir := filepath.Join(OutDirBase, work.DeviceId)
	if err = gocommons.Makedirs(outdir); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to create output directory", err)
		return err
	}
	outfile := filepath.Join(OutDirBase, work.DeviceId, work.BootId+".gz")
	if work.OutFile, err = gocommons.Open(outfile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, gocommons.GZ_TRUE); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to open output file", err)
		return err
	}
	if work.OutWriter, err = work.OutFile.Writer(1048576); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to get writer to outfile", err)
		return err
	}
	return err
}

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
}

func ReadFileInReverse(file *os.File, lineChannel chan string, commChannel chan struct{}) error {
	var fileSize int64
	var err error

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
