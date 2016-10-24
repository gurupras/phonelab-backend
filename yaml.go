package phonelab_backend

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/gurupras/gocommons"

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

func WriteMetadata(writer io.Writer, metadata interface{}) (err error) {
	buf := new(bytes.Buffer)
	metadataBytes, err := yaml.Marshal(metadata)
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to marshal metadata into YAML: %v", err))
		return
	}
	totalLen := len(metadataBytes) + 8
	buf.WriteString(fmt.Sprintf("%08d\n", totalLen))
	buf.WriteString("---\n")
	buf.Write(metadataBytes)
	buf.WriteString("---\n")
	if _, err = writer.Write(buf.Bytes()); err != nil {
		err = errors.New(fmt.Sprintf("Failed to write bytes to writer: %v", err))
		return
	}
	return
}

func ParseYamlBytesFromReader(reader io.Reader) (yamlBytes []byte, err error) {
	buf := new(bytes.Buffer)

	if _, err = io.CopyN(buf, reader, 9); err != nil {
		err = errors.New(fmt.Sprintf("Failed to read length of metadata bytes: %v", err))
		return
	}

	var metadataBytesLen int
	if metadataBytesLen, err = strconv.Atoi(strings.TrimSpace(buf.String())); err != nil {
		err = errors.New(fmt.Sprintf("Failed to convert length of metadata bytes (%v) to int: %v", buf.String(), err))
		return
	}

	buf = new(bytes.Buffer)
	_, err = io.CopyN(buf, reader, int64(metadataBytesLen))
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to read data from reader: %v", err))
		return
	}

	yamlBytes = buf.Bytes()
	return
}

func ParseYamlBytesFromFile(filePath string) (yamlBytes []byte, err error) {
	var (
		file       *gocommons.File
		gzipReader *gzip.Reader
	)

	file, err = gocommons.Open(filePath, os.O_RDONLY, gocommons.GZ_UNKNOWN)
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to open staged file to move to pending work: %v", err))
		return
	}
	defer file.Close()

	// First read the first 8 bytes. These contain the size of the metadata bytes
	if gzipReader, err = gzip.NewReader(file.File); err != nil {
		err = errors.New(fmt.Sprintf("Failed to get gzip reader to file '%v': %v", file.Path, err))
		return
	}

	yamlBytes, err = ParseYamlBytesFromReader(gzipReader)
	return
}

func ParseStagingMetadataFromFile(filePath string) (stagingMetadata *StagingMetadata, err error) {
	var metadataBytes []byte

	if metadataBytes, err = ParseYamlBytesFromFile(filePath); err != nil {
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
