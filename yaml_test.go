package phonelab_backend_test

import (
	"bytes"
	"errors"
	"testing"

	"gopkg.in/yaml.v2"

	"github.com/gurupras/phonelab_backend"
	"github.com/stretchr/testify/assert"
)

func generateFakeWork() *phonelab_backend.Work {
	work := &phonelab_backend.Work{}
	work.Version = "1.0"
	work.DeviceId = "dummy"
	work.PackageName = "com.example.test"
	work.UploadTimestamp = 14
	return work
}

func TestWorkToStagingMetadata(t *testing.T) {
	//t.Parallel()

	assert := assert.New(t)

	defer Recover("TestWorkToStagingMetadata", assert)

	metadata := phonelab_backend.WorkToStagingMetadata(nil)
	assert.Nil(metadata, "Got metadata from nil work")

	work := generateFakeWork()

	metadata = phonelab_backend.WorkToStagingMetadata(work)
	assert.Equal(work.Version, metadata.Version, "Version did not match")
	assert.Equal(work.DeviceId, metadata.DeviceId, "DeviceId did not match")
	assert.Equal(work.PackageName, metadata.PackageName, "PackageName did not match")
	assert.Equal(work.UploadTimestamp, metadata.UploadTimestamp, "UploadTimestamp did not match")
}

func TestGenerateStagingMetadata(t *testing.T) {
	//t.Parallel()

	var metadata []byte

	assert := assert.New(t)

	defer Recover("TestGenerateStagingMetadata", assert)

	work := generateFakeWork()
	metadata = phonelab_backend.GenerateStagingMetadata(work)
	assert.NotNil(metadata, "No metadata from proper work")
}

type DummyWriter string

func (dw *DummyWriter) Write([]byte) (n int, err error) {
	// Just throw error
	return -1, errors.New("Exected")
}

func TestWriteWorkAsYamlMetadataBytes(t *testing.T) {
	//t.Parallel()

	var err error
	var work *phonelab_backend.Work
	var buf bytes.Buffer
	var yamlStruct phonelab_backend.StagingMetadata

	assert := assert.New(t)

	defer Recover("TestWriteWorkAsYamlMetadataBytes", assert)

	work = generateFakeWork()

	// Force the writer to fail for coverage
	dw := new(DummyWriter)
	err = phonelab_backend.WriteWorkAsYamlMetadataBytes(dw, work)
	assert.NotNil(err, "Expected error but got none")

	// Now test valid yaml
	err = phonelab_backend.WriteWorkAsYamlMetadataBytes(&buf, work)
	assert.Nil(err, "Error in writing YAML metadata")

	yamlStruct = phonelab_backend.StagingMetadata{}
	yamlBytes, err := phonelab_backend.ParseYamlBytesFromReader(bytes.NewReader(buf.Bytes()))
	assert.Nil(err, "Failed to parse yaml bytes from buffer")

	err = yaml.Unmarshal(yamlBytes, &yamlStruct)
	assert.Nil(err, "Failed to unmarshal marshalled metadata")

	assert.Equal(work.Version, yamlStruct.Version, "Version did not match")
	assert.Equal(work.DeviceId, yamlStruct.DeviceId, "DeviceId did not match")
	assert.Equal(work.PackageName, yamlStruct.PackageName, "PackageName did not match")
	assert.Equal(work.UploadTimestamp, yamlStruct.UploadTimestamp, "UploadTimestamp did not match")

	// Now try to add data after YAML and see what happens
	payload := "Just some stuff you know"
	var n int
	n, err = buf.WriteString(payload)
	assert.Equal(len(payload), n, "Failed to write all the bytes")
	assert.Nil(err, "Error while adding payload")

	yamlStruct = phonelab_backend.StagingMetadata{}
	yamlBytes, err = phonelab_backend.ParseYamlBytesFromReader(bytes.NewReader(buf.Bytes()))
	assert.Nil(err, "Failed to parse yaml bytes from buffer")

	err = yaml.Unmarshal(yamlBytes, &yamlStruct)
	assert.Nil(err, "Failed to unmarshal marshalled metadata")

	assert.Equal(work.Version, yamlStruct.Version, "Version did not match")
	assert.Equal(work.DeviceId, yamlStruct.DeviceId, "DeviceId did not match")
	assert.Equal(work.PackageName, yamlStruct.PackageName, "PackageName did not match")
	assert.Equal(work.UploadTimestamp, yamlStruct.UploadTimestamp, "UploadTimestamp did not match")
}
