package phonelab_backend

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v2"
)

func WorkToStagingMetadata(work *Work) *StagingMetadata {
	if work == nil {
		return nil
	}

	metadata := &StagingMetadata{}
	metadata.UploadMetadata = work.UploadMetadata
	return metadata
}

func GenerateStagingMetadata(work *Work) []byte {
	// The following statement __cannot__ fail
	metadata, _ := yaml.Marshal(WorkToStagingMetadata(work))
	return metadata
}

func WriteMetadata(writer io.Writer, metadata interface{}) (err error) {
	buf := new(bytes.Buffer)
	metadataBytes, err := yaml.Marshal(metadata)
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to marshal metadata into YAML: %v", err))
		return
	}
	buf.WriteString("---\n")
	buf.Write(metadataBytes)
	buf.WriteString("---\n")
	if _, err = writer.Write(buf.Bytes()); err != nil {
		err = errors.New(fmt.Sprintf("Failed to write bytes to writer: %v", err))
		return
	}
	return
}

func WriteWorkAsYamlMetadataBytes(writer io.Writer, work *Work) (err error) {
	if err = WriteMetadata(writer, WorkToStagingMetadata(work)); err != nil {
		err = errors.New(fmt.Sprintf("Failed WriteWorkAsYamlMetadataBytes(): %v", err))
		return
	}
	return
}
