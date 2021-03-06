package phonelab_backend

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/fatih/set"
	"github.com/gurupras/gocommons"
)

var ()

type ProcessingWork struct {
	WorkList       []*Work
	DeviceId       string
	OutFile        *gocommons.File
	StartTimestamp int64
	EndTimestamp   int64
}

type OutMetadata struct {
	Versions         []string `yaml:versions`
	DeviceId         string   `yaml:device_id`
	PackageNames     []string `yaml:package_names`
	UploadTimestamps []int64  `yaml:upload_timestamps`
	BootIds          []string `yaml:boot_ids`
	Tags             []string `yaml:tags`
}

type ProcessingFunction func(work *ProcessingWork) (error, bool)

type ProcessingConfig struct {
	PreProcessing         []ProcessingFunction
	Core                  func(processingWork *ProcessingWork, processingConfig *ProcessingConfig) error
	PostProcessing        []ProcessingFunction
	DelayBeforeProcessing time.Duration
	WorkSetCheckPeriod    time.Duration
}

func InitializeProcessingConfig() *ProcessingConfig {
	pc := new(ProcessingConfig)

	pc.Core = ProcessStagedWork

	return pc
}

func NewOutMetadata() *OutMetadata {
	ret := new(OutMetadata)
	ret.Versions = make([]string, 0)
	ret.PackageNames = make([]string, 0)
	ret.UploadTimestamps = make([]int64, 0)
	ret.BootIds = make([]string, 0)
	ret.Tags = make([]string, 0)
	return ret
}

func ProcessStage(functions []ProcessingFunction, work *ProcessingWork) (errs []error, fail bool) {
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

func ProcessProcessConfig(workList []*Work, processingConfig *ProcessingConfig) (err error) {
	if workList == nil || len(workList) == 0 {
		err = errors.New(fmt.Sprintf("No work to process..why was this function called with no work?"))
		return
	}

	processingWork := &ProcessingWork{
		WorkList: workList,
		DeviceId: workList[0].DeviceId,
	}

	var errs []error
	var fail bool
	//fmt.Println("Starting pre-processing")
	if errs, fail = ProcessStage(processingConfig.PreProcessing, processingWork); len(errs) > 0 && fail {
		err = errors.New(fmt.Sprintf("Stopping ProcessProcessConfig due to fail condition...\nerrors:\n%v\n", errs))
		return
	}
	//logger.Debugln(fmt.Sprintf("%s -> Finished pre-processing", processingWork.DeviceId))

	// FIXME: For some reason TestUpload is stuck in this
	if err = processingConfig.Core(processingWork, processingConfig); err != nil {
		err = errors.New(fmt.Sprintf("Failed core processing: %v", err))
		return
	}
	//logger.Debugln(fmt.Sprintf("%s -> Finished core", processingWork.DeviceId))

	if errs, fail = ProcessStage(processingConfig.PostProcessing, processingWork); len(errs) > 0 && fail {
		err = errors.New(fmt.Sprintf("Stopping ProcessProcessConfig due to fail condition...\nerrors:\n%v\n", errs))
		return
	}
	//logger.Debugln(fmt.Sprintf("%s -> Finished post-processing", processingWork.DeviceId))
	return
}

func ProcessStagedWork(processingWork *ProcessingWork, processingConfig *ProcessingConfig) (err error) {
	// Zero length/nil WorkList had better been caught and thrown by now

	// First, we need to sort each chunk in the WorkList
	// Then we update the metadata on the output file
	// Then we do an n-way merge between all the chunks to produce an output file for a date
	// TODO: Then, we move it to wherever it is supposed to go

	// Date corresponding to this work
	date := processingWork.WorkList[0].StagingMetadata.Dates[0]

	outDirBase := processingWork.WorkList[0].OutDir
	deviceOutDir := filepath.Join(outDirBase, processingWork.DeviceId)
	// Final out dir
	yearStr := fmt.Sprintf("%v", date.Year())
	monthStr := fmt.Sprintf("%v", date.Month())
	dayStr := fmt.Sprintf("%v", date.Day())
	resultOutDir := filepath.Join(deviceOutDir, yearStr, monthStr)
	if err = gocommons.Makedirs(resultOutDir); err != nil {
		err = errors.New(fmt.Sprintf("Failed to create resultOutDir(%s): %v", resultOutDir, err))
		return
	}

	sortedDirBase := filepath.Join(processingWork.WorkList[0].StagingDir, "sorted")
	if err = gocommons.Makedirs(sortedDirBase); err != nil {
		err = errors.New(fmt.Sprintf("Failed to create directory for sorted files: %v", err))
		return err
	}

	outMetadata := NewOutMetadata()

	outMetadata.DeviceId = processingWork.DeviceId

	sortedFiles := make([]string, 0)
	for _, chunkWork := range processingWork.WorkList {
		fileName := filepath.Base(chunkWork.StagingFileName)
		sortedFile := filepath.Join(sortedDirBase, fileName)

		// Update outMetadata
		outMetadata.Versions = append(outMetadata.Versions, chunkWork.Version)
		outMetadata.UploadTimestamps = append(outMetadata.UploadTimestamps, chunkWork.UploadTimestamp)
		outMetadata.PackageNames = append(outMetadata.PackageNames, chunkWork.PackageName)

		if err = SortLogs(chunkWork.StagingFileName, sortedFile); err != nil {
			err = errors.New(fmt.Sprintf("Failed sortLogs(%v, %v): %v", chunkWork.StagingFileName, sortedFile, err))
			return
		}

		sortedFiles = append(sortedFiles, sortedFile)
	}

	// Set up the output file for writing
	var (
		exists bool
		ofile  *gocommons.File
		writer gocommons.Writer
	)

	outPath := filepath.Join(resultOutDir, dayStr+".gz")
	if exists, err = gocommons.Exists(outPath); err != nil {
		err = errors.New(fmt.Sprintf("Failed to check if outPath(%s) exists: %v", outPath, err))
		return
	} else if exists {
		//err = errors.New(fmt.Sprintf("WARNING! outPath(%s) already exists? We're getting log files for a date after that date was already processed!", outPath))
		//return

		// File already exists..
		// Change outPath i guess
		var tmpfile *os.File
		if tmpfile, err = gocommons.TempFile(resultOutDir, "dayStr-", ".gz"); err != nil {
			err = errors.New(fmt.Sprintf("Failed to create a secondary out file since primary outfile exists: %v", err))
			return
		}
		tmpfile.Close()
		outPath = tmpfile.Name()
	}

	if ofile, err = gocommons.Open(outPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, gocommons.GZ_TRUE); err != nil {
		err = errors.New(fmt.Sprintf("Failed to open outPath(%s) for writing sorted log data: %v", outPath, err))
		return
	}
	if writer, err = ofile.Writer(0); err != nil {
		err = errors.New(fmt.Sprintf("Failed to get writer to outPath(%s) for writing sorted log data: %v", outPath, err))
		return
	}
	defer writer.Close()
	defer writer.Flush()

	/**
	 * Combine the metadata from multiple work instances
	 */
	tags := set.NewNonTS()
	bootIds := set.NewNonTS()
	for _, chunkWork := range processingWork.WorkList {
		for _, tag := range chunkWork.StagingMetadata.Tags {
			tags.Add(tag)
		}
		for _, bootId := range chunkWork.StagingMetadata.BootIds {
			bootIds.Add(bootId)
		}
	}
	for _, data := range tags.List() {
		tag := data.(string)
		outMetadata.Tags = append(outMetadata.Tags, tag)
	}
	for _, data := range bootIds.List() {
		bootId := data.(string)
		outMetadata.BootIds = append(outMetadata.BootIds, bootId)
	}

	// Write the metadata
	if err = WriteMetadata(writer, outMetadata); err != nil {
		err = errors.New(fmt.Sprintf("Failed to write outmetadata to file '%v': %v", outPath, err))
		return
	}

	// Now we have a bunch of chunks..we should be able to call n-way merge on this
	sortedChannel := make(chan gocommons.SortInterface, 1000)
	sortParams := *NewLoglineSortParams()
	nWayMergeCallback := func(sortedChannel chan gocommons.SortInterface, quit chan bool) {
		first := true
		for {
			obj, ok := <-sortedChannel
			if !ok {
				break
			}

			logline := obj.(*Logline)

			// Add new line after every line
			if first {
				// First line..don't prefix newline
				writer.Write([]byte(logline.String()))
				first = false
			} else {
				writer.Write([]byte("\n" + logline.String()))
			}
		}
		close(quit)
	}
	if err = gocommons.NWayMergeGenerator(sortedFiles, sortParams, sortedChannel, nWayMergeCallback); err != nil {
		err = errors.New(fmt.Sprintf("Failed NWayMergeGenerator(): %v", err))
	}
	return
}
