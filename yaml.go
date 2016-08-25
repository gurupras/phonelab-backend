package phonelab_backend

import (
	"bytes"
	"fmt"
	"io"

	"gopkg.in/yaml.v2"
)

type StagingMetadata struct {
	Version         string `yaml:version`
	DeviceId        string `yaml:device_id`
	PackageName     string `yaml:package_name`
	UploadTimestamp int64  `yaml:upload_timestamp`
}

func WorkToStagingMetadata(work *Work) *StagingMetadata {
	if work == nil {
		return nil
	}

	metadata := &StagingMetadata{
		Version:         work.Version,
		DeviceId:        work.DeviceId,
		PackageName:     work.PackageName,
		UploadTimestamp: work.UploadTimestamp,
	}
	return metadata
}

func GenerateStagingMetadata(work *Work) []byte {
	// The following statement __cannot__ fail
	metadata, _ := yaml.Marshal(WorkToStagingMetadata(work))
	return metadata
}

func WriteStagingMetadata(writer io.Writer, work *Work) (err error) {
	var metadata []byte

	metadata = GenerateStagingMetadata(work)

	buf := new(bytes.Buffer)
	buf.Write(metadata)
	metadataStr := buf.String()
	if _, err := writer.Write([]byte(fmt.Sprintf("---\n%v---\n", metadataStr))); err != nil {
		return err
	}
	return
}
