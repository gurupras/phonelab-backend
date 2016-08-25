package phonelab_backend

import (
	"bytes"
	"testing"

	"gopkg.in/yaml.v2"

	"github.com/stretchr/testify/assert"
)

func generateFakeWork() *Work {
	work := &Work{
		Version:         "1.0",
		DeviceId:        "dummy",
		PackageName:     "com.example.test",
		UploadTimestamp: 14,
	}
	return work
}

func TestWorkToStagingMetadata(t *testing.T) {
	assert := assert.New(t)

	work := generateFakeWork()

	metadata := WorkToStagingMetadata(work)
	assert.Equal(metadata.Version, "1.0", "Version did not match")
	assert.Equal(metadata.DeviceId, "dummy", "DeviceId did not match")
	assert.Equal(metadata.PackageName, "com.example.test", "PackageName did not match")
	assert.Equal(metadata.UploadTimestamp, int64(14), "UploadTimestamp did not match")
}

func TestGenerateStagingMetadata(t *testing.T) {
	var metadata []byte

	assert := assert.New(t)

	work := generateFakeWork()
	metadata = GenerateStagingMetadata(work)
	assert.NotNil(metadata, "No metadata from proper work")
}

func TestWriteStagingMetadata(t *testing.T) {
	var err error
	var work *Work
	var buf bytes.Buffer
	var yamlStruct StagingMetadata

	assert := assert.New(t)

	work = generateFakeWork()

	err = WriteStagingMetadata(&buf, work)
	assert.Nil(err, "Error in writing YAML metadata")

	yamlStruct = StagingMetadata{}
	err = yaml.Unmarshal(buf.Bytes(), &yamlStruct)
	assert.Nil(err, "Failed to unmarshal marshalled metadata")

	assert.Equal(yamlStruct.Version, "1.0", "Version did not match")
	assert.Equal(yamlStruct.DeviceId, "dummy", "DeviceId did not match")
	assert.Equal(yamlStruct.PackageName, "com.example.test", "PackageName did not match")
	assert.Equal(yamlStruct.UploadTimestamp, int64(14), "UploadTimestamp did not match")

	// Now try to add data after YAML and see what happens
	payload := "Just some stuff you know"
	var n int
	n, err = buf.WriteString(payload)
	assert.Equal(len(payload), n, "Failed to write all the bytes")
	assert.Nil(err, "Error while adding payload")

	yamlStruct = StagingMetadata{}
	err = yaml.Unmarshal(buf.Bytes(), &yamlStruct)
	assert.Nil(err, "Failed to unmarshal marshalled metadata")

	assert.Equal(yamlStruct.Version, "1.0", "Version did not match")
	assert.Equal(yamlStruct.DeviceId, "dummy", "DeviceId did not match")
	assert.Equal(yamlStruct.PackageName, "com.example.test", "PackageName did not match")
	assert.Equal(yamlStruct.UploadTimestamp, int64(14), "UploadTimestamp did not match")

}
