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

func ParseYamlBytesFromFile(filePath string, maxHeaderSize int) (yamlBytes []byte, err error) {
	var file *os.File

	file, err = os.OpenFile(filePath, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to open staged file to move to pending work\n", err)
		return
	}
	// XXX: Hard-coded to 1K
	buf := new(bytes.Buffer)

	_, err = io.CopyN(buf, file, 1024)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to read data from staged file\n", err)
		return
	}

	var gzipReader *gzip.Reader
	if gzipReader, err = gzip.NewReader(buf); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to obtain reader to compressed stream", err)
		return
	}
	uncompressedBuf := new(bytes.Buffer)
	io.Copy(uncompressedBuf, gzipReader)

	yamlBytes = uncompressedBuf.Bytes()
	return
}

func ParseStagingMetadataFromFile(filePath string, nBytes ...int) (stagingMetadata *StagingMetadata, err error) {
	var maxHeaderSize int
	var metadataBytes []byte

	if len(nBytes) == 0 {
		maxHeaderSize = 1024
	} else {
		maxHeaderSize = nBytes[0]
		if maxHeaderSize <= 0 {
			maxHeaderSize = 1024
		}
	}

	if metadataBytes, err = ParseYamlBytesFromFile(filePath, maxHeaderSize); err != nil {
		return
	}

	stagingMetadata = new(StagingMetadata)
	err = yaml.Unmarshal(metadataBytes, stagingMetadata)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to unmarshall staging metadata\n", err)
		return
	}
	return
}
