package phonelab_backend_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v2"

	"github.com/gurupras/gocommons"
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

func testEquality(work *phonelab_backend.Work, reader io.Reader, assert *assert.Assertions) {
	yamlStruct := phonelab_backend.StagingMetadata{}
	yamlBytes, err := phonelab_backend.ParseYamlBytesFromReader(reader)
	assert.Nil(err, "Failed to parse yaml bytes from buffer")
	err = yaml.Unmarshal(yamlBytes, &yamlStruct)
	assert.Nil(err, "Failed to unmarshal marshalled metadata")
	assert.Equal(work.Version, yamlStruct.Version, "Version did not match")
	assert.Equal(work.DeviceId, yamlStruct.DeviceId, "DeviceId did not match")
	assert.Equal(work.PackageName, yamlStruct.PackageName, "PackageName did not match")
	assert.Equal(work.UploadTimestamp, yamlStruct.UploadTimestamp, "UploadTimestamp did not match")
}

type DummyWriter string

func (dw *DummyWriter) Write([]byte) (n int, err error) {
	// Just throw error
	return -1, errors.New("Exected")
}

func TestWriteMetadata(t *testing.T) {
	//t.Parallel()

	var err error
	var work *phonelab_backend.Work
	var buf bytes.Buffer

	assert := assert.New(t)

	defer Recover("TestWriteMetadata", assert)

	work = generateFakeWork()

	// Force the writer to fail for coverage
	dw := new(DummyWriter)
	err = phonelab_backend.WriteMetadata(dw, work.StagingMetadata)
	assert.NotNil(err, "Expected error but got none")

	// Now test valid yaml
	err = phonelab_backend.WriteMetadata(&buf, work.StagingMetadata)
	assert.Nil(err, "Error in writing YAML metadata")
	testEquality(work, bytes.NewReader(buf.Bytes()), assert)

	// Now try to add data after YAML and see what happens
	payload := "Just some stuff you know"
	var n int
	n, err = buf.WriteString(payload)
	assert.Equal(len(payload), n, "Failed to write all the bytes")
	assert.Nil(err, "Error while adding payload")
	testEquality(work, bytes.NewReader(buf.Bytes()), assert)
}

func TestParseYamlBytesFromReader(t *testing.T) {
	//t.Parallel()

	var err error
	assert := assert.New(t)

	ibuf := new(bytes.Buffer)

	// First, fail on io.CopyN(buf, reader, 9)
	ibuf.Reset()
	_, err = phonelab_backend.ParseYamlBytesFromReader(bytes.NewReader(ibuf.Bytes()))
	assert.NotNil(err, "Expected failure on empty reader")

	// Now, have bytes, but fail strconv.Atoi()
	ibuf.Reset()
	ibuf.WriteString(fmt.Sprintf("%04d\nabcd", 1024))
	_, err = phonelab_backend.ParseYamlBytesFromReader(bytes.NewReader(ibuf.Bytes()))
	assert.NotNil(err, "Expected failure on invalid length header")

	// Now, have a size, but bytes that don't match that size
	ibuf.Reset()
	ibuf.WriteString(fmt.Sprintf("%08d\nabcd", 1024))
	_, err = phonelab_backend.ParseYamlBytesFromReader(bytes.NewReader(ibuf.Bytes()))
	assert.NotNil(err, "Expected failure on missing data bytes")

	// Now, have everything valid and test for success
	ibuf.Reset()
	// Write valid metadata to buffer
	work := generateFakeWork()
	err = phonelab_backend.WriteMetadata(ibuf, work.StagingMetadata)
	assert.Nil(err, "Failed to write metadata to buffer")
	// Parse it back out and check for success
	testEquality(work, bytes.NewReader(ibuf.Bytes()), assert)
}

func TestParseYamlBytesFromFile(t *testing.T) {
	//t.Parallel()

	var err error
	assert := assert.New(t)

	// First, fail on a file that doesn't exist
	filePath := filepath.Join(testDirBase, "filepaththatdoesnotexist")
	_, err = phonelab_backend.ParseYamlBytesFromFile(filePath)
	assert.NotNil(err, "Expected error on file that does not exist")

	// Now, fail on a file that exists but doesn't contain valid metadata
	// In this case, we just use an empty file
	f, err := gocommons.TempFile(testDirBase, "staging-testparseyamlbytesfromfile-")
	assert.Nil(err, "Failed to create a temporary file")
	f.Close()
	filePath = f.Name()

	_, err = phonelab_backend.ParseYamlBytesFromFile(filePath)
	assert.NotNil(err, "Expected error on empty file that does not contain metadata")

	// TODO: Now for validity
	work := generateFakeWork()
	f, err = os.OpenFile(filePath, os.O_WRONLY, 0664)
	assert.Nil(err, "Failed to open file")
	err = phonelab_backend.WriteMetadata(f, work.StagingMetadata)
	assert.Nil(err, "Failed to write metadata to file")
	f.Close()
	// Now read it back and verify that they match
	f, err = os.OpenFile(filePath, os.O_RDONLY, 0664)
	assert.Nil(err, "Failed to open file")
	testEquality(work, f, assert)

	os.Remove(f.Name())
}

func TestParseStagingMetadataFromFile(t *testing.T) {
	//t.Parallel()

	var err error
	assert := assert.New(t)

	// First, fail on a file that doesn't exist
	filePath := filepath.Join(testDirBase, "filepaththatdoesnotexist")
	_, err = phonelab_backend.ParseStagingMetadataFromFile(filePath)
	assert.NotNil(err, "Expected error on file that does not exist")

	// Now, fail yaml.Unmarshal
	// We do this by writing valid yaml data into the file, but
	// yaml data that will not unmarshal into StagingMetadata.
	// First, open file
	f, err := gocommons.TempFile(testDirBase, "staging-testparsestagingmetadatafromfile-")
	dummyString := "dummy string that should fail unmarshal"
	f.WriteString(fmt.Sprintf("%08d\n%s", len(dummyString), dummyString))
	// Now, fail yaml.Unmarshal
	_, err = phonelab_backend.ParseStagingMetadataFromFile(f.Name())
	assert.NotNil(err, "Expected failure while unmarshalling map into StagingMetadata")
	f.Close()

	// Now, success
	work := generateFakeWork()
	filePath = f.Name()
	f, err = os.OpenFile(filePath, os.O_WRONLY, 0664)
	assert.Nil(err, "Failed to open file")
	err = phonelab_backend.WriteMetadata(f, work.StagingMetadata)
	assert.Nil(err, "Failed to write metadata to file")
	f.Close()
	// Now read it back and verify that they match
	f, err = os.OpenFile(filePath, os.O_RDONLY, 0664)
	assert.Nil(err, "Failed to open file")
	testEquality(work, f, assert)

	os.Remove(f.Name())
}
