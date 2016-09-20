package phonelab_backend_test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gurupras/gocommons"
	"github.com/gurupras/phonelab_backend"
	"github.com/jehiah/go-strftime"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

type RandomLoglineGenerator struct {
	BootId                  string
	StartTimestamp          time.Time
	LastLogcatTimestamp     time.Time
	LastLogcatToken         int64
	MaxDelayBetweenLoglines time.Duration
}

func GenerateRandomBootId() string {
	return uuid.NewV4().String()
}

func GenerateTag() string {
	unallowed := ":"

	tag := fmt.Sprintf("DummyTag->%s:", GenerateRandomString(1+rand.Intn(32), false, unallowed))
	return tag
}

// TODO: Combine GenerateRandomLogline and GenerateLoglineForPayload
func GenerateRandomLogline(rlg *RandomLoglineGenerator) string {
	tokens := make([]string, 0)

	tokens = append(tokens, rlg.BootId)

	maxDelayNanos := rlg.MaxDelayBetweenLoglines.Nanoseconds()
	delay := time.Duration(rand.Int63n(maxDelayNanos))
	loglineTimestamp := rlg.LastLogcatTimestamp.Add(delay)
	tokens = append(tokens, fmt.Sprintf("%s.%09d", strftime.Format("%Y-%m-%d %H:%M:%S", loglineTimestamp), loglineTimestamp.Nanosecond()))

	tokens = append(tokens, fmt.Sprintf("%v", rlg.LastLogcatToken+1))

	offset := float64((loglineTimestamp.UnixNano() - rlg.StartTimestamp.UnixNano()))
	tokens = append(tokens, fmt.Sprintf("[%.06f]", offset/1e6))

	tokens = append(tokens, fmt.Sprintf("%v", rand.Int31n(32768)))
	tokens = append(tokens, fmt.Sprintf("%v", rand.Int31n(32768)))

	levels := []string{"V", "D", "I", "W", "E", "C", "WTF"}
	tokens = append(tokens, levels[rand.Intn(len(levels))])

	tokens = append(tokens, GenerateTag())

	payload := GenerateRandomString(32+rand.Intn(256), true, "")
	tokens = append(tokens, payload)

	// Now update rlg
	rlg.LastLogcatTimestamp = loglineTimestamp
	rlg.LastLogcatToken++

	return strings.Join(tokens, "\t") + "\n"
}

func GenerateLoglineForPayload(payload string) string {
	timeOffset := time.Now().UnixNano()
	tokenNumber := int64(322222)

	tokens := make([]string, 0)

	bootId := GenerateRandomBootId()
	tokens = append(tokens, bootId)

	timeNow := time.Now()
	tokens = append(tokens, fmt.Sprintf("%s.%09d", strftime.Format("%Y-%m-%d %H:%M:%S", timeNow), timeNow.Nanosecond()))

	tokens = append(tokens, fmt.Sprintf("%v", tokenNumber))

	offset := float64((timeNow.UnixNano() - timeOffset))
	tokens = append(tokens, fmt.Sprintf("[%v]", offset/1e6))

	tokens = append(tokens, fmt.Sprintf("%v", rand.Int31n(32768)))
	tokens = append(tokens, fmt.Sprintf("%v", rand.Int31n(32768)))

	levels := []string{"V", "D", "I", "W", "E", "C", "WTF"}
	tokens = append(tokens, levels[rand.Intn(len(levels))])

	tokens = append(tokens, GenerateTag())

	tokens = append(tokens, payload)

	return `` + strings.Join(tokens, "\t") + "\n"
}

func GenerateRandomString(length int, allowSpaces bool, unallowed string) string {
	var letterBytes string = `abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789!@#$%^&*()_+[]{};:'",<.>/?\|`
	var spaceBytes string = ` 	`

	validBuf := new(bytes.Buffer)
	banBuf := new(bytes.Buffer)

	for _, runeByte := range unallowed {
		banBuf.WriteString(string(runeByte))
	}

	for _, runeBytes := range letterBytes {
		if !strings.Contains(banBuf.String(), string(runeBytes)) {
			validBuf.WriteString(string(runeBytes))
		}
	}

	if allowSpaces {
		for _, runeBytes := range spaceBytes {
			if !strings.Contains(banBuf.String(), string(runeBytes)) {
				validBuf.WriteString(string(runeBytes))
			}
		}
	}

	b := make([]byte, length)

	validBytes := validBuf.String()
	for i := range b {
		b[i] = validBytes[rand.Intn(len(validBytes))]
	}
	return string(b)
}

func TestGenerateBootId(t *testing.T) {
	//t.Parallel()

	assert := assert.New(t)

	pattern := regexp.MustCompile(`(?P<boot_id>[a-z0-9\-]{36})`)
	for i := 0; i < 1000; i++ {
		id := GenerateRandomBootId()
		values_raw := pattern.FindAllStringSubmatch(id, -1)
		assert.NotEqual(nil, values_raw, "Failed FindAllStringSubmatch()")
	}
}

func TestGenerateRandomLogline(t *testing.T) {
	//t.Parallel()

	assert := assert.New(t)

	var err error
	var logline *phonelab_backend.Logline
	var line string

	rlg := new(RandomLoglineGenerator)
	rlg.BootId = GenerateRandomBootId()
	rlg.StartTimestamp = time.Now()
	rlg.LastLogcatTimestamp = rlg.StartTimestamp
	rlg.MaxDelayBetweenLoglines = 3 * time.Minute

	for i := 0; i < 10000; i++ {
		line = GenerateRandomLogline(rlg)
		if logline, err = phonelab_backend.ParseLogline(line); err != nil || logline == nil {
			break
		}
	}
	assert.Nil(err, fmt.Sprintf("Failed to parse logline: %v\n%v\n", err, line))
	assert.NotNil(logline, fmt.Sprintf("No error but logline was nil?\n%v\n", line))
}

func TestCheckLogcatPattern(t *testing.T) {
	//t.Parallel()
	assert := assert.New(t)

	defer Recover("TestCheckLogcatPattern", assert)

	var logline *phonelab_backend.Logline
	var err error

	// Empty string
	line := ""
	logline, err = phonelab_backend.ParseLogline(line)
	assert.Nil(logline, "Obtained LogLine from empty string")
	assert.NotNil(err, "Should have got error")

	// Illegal DatetimeNanos
	line = "6b793913-7cd9-477a-bbfa-62f07fbac87b 2016-04-21 09:51:01.1990250000000000000000000000000000000000000000000000000638 11553177 [29981.752359]   202   203 D Kernel-Trace:      kworker/1:1-21588 [001] ...2 29981.751893: phonelab_periodic_ctx_switch_info: cpu=1 pid=7641 tgid=7613 nice=0 comm=Binder_1 utime=0 stime=0 rtime=158906 bg_utime=0 bg_stime=0 bg_rtime=0 s_run=0 s_int=2 s_unint=0 s_oth=0 log_idx=79981"
	logline, err = phonelab_backend.ParseLogline(line)
	assert.Nil(logline, "Obtained LogLine despite illegal DatetimeNanos")
	assert.NotNil(err, "Should have got error")

	// Illegal Datetime
	line = "6b793913-7cd9-477a-bbfa-62f07fbac87b 2016-04-21 09:61:01.199025638 11553177 [29981.752359]   202   203 D Kernel-Trace:      kworker/1:1-21588 [001] ...2 29981.751893: phonelab_periodic_ctx_switch_info: cpu=1 pid=7641 tgid=7613 nice=0 comm=Binder_1 utime=0 stime=0 rtime=158906 bg_utime=0 bg_stime=0 bg_rtime=0 s_run=0 s_int=2 s_unint=0 s_oth=0 log_idx=79981"
	logline, err = phonelab_backend.ParseLogline(line)
	assert.Nil(logline, "Obtained LogLine despite illegal Datetime")
	assert.NotNil(err, "Should have got error")
	// Legal Datetime with short micros
	line = "6b793913-7cd9-477a-bbfa-62f07fbac87b 2016-04-21 09:51:01.1 11553177 [29981.752359]   202   203 D Kernel-Trace:      kworker/1:1-21588 [001] ...2 29981.751893: phonelab_periodic_ctx_switch_info: cpu=1 pid=7641 tgid=7613 nice=0 comm=Binder_1 utime=0 stime=0 rtime=158906 bg_utime=0 bg_stime=0 bg_rtime=0 s_run=0 s_int=2 s_unint=0 s_oth=0 log_idx=79981"
	logline, err = phonelab_backend.ParseLogline(line)
	assert.NotNil(logline, "Failed to obtain LogLine despite legal Datetime")
	// Legal Datetime with long micros
	line = "6b793913-7cd9-477a-bbfa-62f07fbac87b 2016-04-21 09:51:01.199025638 11553177 [29981.752359]   202   203 D Kernel-Trace:      kworker/1:1-21588 [001] ...2 29981.751893: phonelab_periodic_ctx_switch_info: cpu=1 pid=7641 tgid=7613 nice=0 comm=Binder_1 utime=0 stime=0 rtime=158906 bg_utime=0 bg_stime=0 bg_rtime=0 s_run=0 s_int=2 s_unint=0 s_oth=0 log_idx=79981"
	logline, err = phonelab_backend.ParseLogline(line)
	assert.NotNil(logline, "Failed to obtain LogLine despite legal Datetime")
	assert.Nil(err, "Got error despite valid logline", err)
	// Illegal LocatToken
	line = "6b793913-7cd9-477a-bbfa-62f07fbac87b 2016-04-21 09:59:01.199025638 11500000000000000000000000000000000000053177 [29981.752359]   202   203 D Kernel-Trace:      kworker/1:1-21588 [001] ...2 29981.751893: phonelab_periodic_ctx_switch_info: cpu=1 pid=7641 tgid=7613 nice=0 comm=Binder_1 utime=0 stime=0 rtime=158906 bg_utime=0 bg_stime=0 bg_rtime=0 s_run=0 s_int=2 s_unint=0 s_oth=0 log_idx=79981"
	logline, err = phonelab_backend.ParseLogline(line)
	assert.Nil(logline, "Obtained LogLine despite illegal LogcatToken")
	assert.NotNil(err, "Should have got error")

	// Illegal Pid
	line = "6b793913-7cd9-477a-bbfa-62f07fbac87b 2016-04-21 09:59:01.199025638 11553177 [29981.752359]   20000000000000000000002   203 D Kernel-Trace:      kworker/1:1-21588 [001] ...2 29981.751893: phonelab_periodic_ctx_switch_info: cpu=1 pid=7641 tgid=7613 nice=0 comm=Binder_1 utime=0 stime=0 rtime=158906 bg_utime=0 bg_stime=0 bg_rtime=0 s_run=0 s_int=2 s_unint=0 s_oth=0 log_idx=79981"
	logline, err = phonelab_backend.ParseLogline(line)
	assert.Nil(logline, "Obtained LogLine despite illegal Pid")
	assert.NotNil(err, "Should have got error")

	// Illegal Tid
	line = "6b793913-7cd9-477a-bbfa-62f07fbac87b 2016-04-21 09:59:01.199025638 11553177 [29981.752359]   202   2000000000000000000000003 D Kernel-Trace:      kworker/1:1-21588 [001] ...2 29981.751893: phonelab_periodic_ctx_switch_info: cpu=1 pid=7641 tgid=7613 nice=0 comm=Binder_1 utime=0 stime=0 rtime=158906 bg_utime=0 bg_stime=0 bg_rtime=0 s_run=0 s_int=2 s_unint=0 s_oth=0 log_idx=79981"
	logline, err = phonelab_backend.ParseLogline(line)
	assert.Nil(logline, "Obtained LogLine despite illegal Tid")
	assert.NotNil(err, "Should have got error")

	line = "6b793913-7cd9-477a-bbfa-62f07fbac87b 2016-04-21 09:59:01.199025638 11553177 [29981.752359]   202   203 D Kernel-Trace:      kworker/1:1-21588 [001] ...2 29981.751893: phonelab_periodic_ctx_switch_info: cpu=1 pid=7641 tgid=7613 nice=0 comm=Binder_1 utime=0 stime=0 rtime=158906 bg_utime=0 bg_stime=0 bg_rtime=0 s_run=0 s_int=2 s_unint=0 s_oth=0 log_idx=79981"
	logline, err = phonelab_backend.ParseLogline(line)
	payload := "kworker/1:1-21588 [001] ...2 29981.751893: phonelab_periodic_ctx_switch_info: cpu=1 pid=7641 tgid=7613 nice=0 comm=Binder_1 utime=0 stime=0 rtime=158906 bg_utime=0 bg_stime=0 bg_rtime=0 s_run=0 s_int=2 s_unint=0 s_oth=0 log_idx=79981"

	assert.NotEqual(nil, logline, "Failed to parse logline")
	assert.Nil(err, "Got error despite valid logline", err)
	assert.Equal("6b793913-7cd9-477a-bbfa-62f07fbac87b", logline.BootId, "BootId was not parsed properly")
	assert.Equal("2016-04-21 09:59:01", strftime.Format("%Y-%m-%d %H:%M:%S", logline.Datetime), "Datetime was not parsed properly")
	assert.Equal(int64(199025638), logline.DatetimeNanos, "DatetimeNanos was not parsed properly")
	assert.Equal(int64(11553177), logline.LogcatToken, "LogcatToken was not parsed properly")
	assert.Equal(29981.752359, logline.TraceTime, "TraceTime was not parsed properly")
	assert.Equal(int32(202), logline.Pid, "Pid was not parsed properly")
	assert.Equal(int32(203), logline.Tid, "Tid was not parsed properly")
	assert.Equal("D", logline.Level, "Level was not parsed properly")
	assert.Equal("Kernel-Trace", logline.Tag, "Tag was not parsed properly")
	assert.Equal(payload, logline.Payload, "Payload was not parsed properly")
}

func TestSortLogs(t *testing.T) {
	//t.Parallel()

	assert := assert.New(t)

	var err error

	dir := filepath.Join(testDirBase, "staging-TestSortLogs")

	err = gocommons.Makedirs(dir)
	assert.Nil(err, "Failed to create temp dir", err)

	var (
		ifile      *os.File
		gzipWriter *gzip.Writer
		ofile      *os.File
		gzipReader *gzip.Reader
	)

	filePath := filepath.Join(dir, "input.gz")
	ifile, err = os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0664)
	assert.Nil(err, "Failed to create input file", err)
	gzipWriter = gzip.NewWriter(ifile)

	rlg := new(RandomLoglineGenerator)
	rlg.BootId = GenerateRandomBootId()
	rlg.LastLogcatToken = 0
	rlg.StartTimestamp = time.Now()
	rlg.LastLogcatTimestamp = rlg.StartTimestamp
	rlg.MaxDelayBetweenLoglines = 10 * time.Second

	// Generate about 1000 loglines
	lines := make([]string, 0)
	for i := 0; i < 1000; i++ {
		lines = append(lines, strings.TrimSpace(GenerateRandomLogline(rlg)))
	}

	// Now mess this up and write it into the file
	modLines := make([]string, 0)
	nonModLines := make([]string, 0)
	for i := 0; i < 1000; i++ {
		if i%5 == 0 {
			modLines = append(modLines, lines[i])
		} else {
			nonModLines = append(nonModLines, lines[i])
		}
	}

	gzipWriter.Write([]byte(strings.Join(modLines, "\n") + "\n"))
	gzipWriter.Write([]byte(strings.Join(nonModLines, "\n")))

	gzipWriter.Flush()
	gzipWriter.Close()
	ifile.Close()

	outPath := filepath.Join(dir, "output.gz")
	err = phonelab_backend.SortLogs(filePath, outPath)
	assert.Nil(err, "Failed to sort", err)

	// Now, read the output file and make sure it matches the string
	// version of lines array

	// This is what is expected
	expected := new(bytes.Buffer)
	expected.WriteString(strings.Join(lines, "\n"))

	origPath := filepath.Join(dir, "orig.gz")
	orig, err := os.OpenFile(origPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0664)
	assert.Nil(err, "Failed to create orig.gz", err)
	gzipWriter = gzip.NewWriter(orig)
	_, err = io.Copy(gzipWriter, expected)
	assert.Nil(err, "Failed to copy to orig.gz", err)
	gzipWriter.Flush()
	gzipWriter.Close()

	expected.Reset()
	expected.WriteString(strings.Join(lines, "\n"))

	ofile, err = os.OpenFile(outPath, os.O_RDONLY, 0664)
	assert.Nil(err, "Failed to open output file", err)
	gzipReader, err = gzip.NewReader(ofile)
	assert.Nil(err, "Failed to get gzip reader to ofile", err)

	obtained := new(bytes.Buffer)
	_, err = io.Copy(obtained, gzipReader)
	assert.Nil(err, "Failed to do io.Copy()", err)

	equal := strings.Compare(expected.String(), obtained.String())
	assert.Equal(0, equal, "Strings don't match")
}

type DummySortInterface int

func (dsi DummySortInterface) Less(s gocommons.SortInterface) (ret bool, err error) {
	odsi := s.(DummySortInterface)
	ret = int(dsi) < int(odsi)
	return
}

func (dsi DummySortInterface) String() string {
	return fmt.Sprintf("%v", dsi)
}

func ParseDummySortInterface(line string) gocommons.SortInterface {
	if val, err := strconv.Atoi(line); err != nil {
		return nil
	} else {
		return DummySortInterface(val)
	}
}
func TestLoglineSortNegative_1(t *testing.T) {
	//t.Parallel()

	assert := assert.New(t)

	var lsp *gocommons.SortParams

	lsp = phonelab_backend.NewLoglineSortParams()
	lsp.LineConvert = ParseDummySortInterface

	line := GenerateLoglineForPayload("dummy")
	loglineSI := phonelab_backend.ParseLoglineToSortInterface(line)
	lsp.Lines = append(lsp.Lines, loglineSI)
	lsp.Lines = append(lsp.Lines, ParseDummySortInterface("4"))

	defer func() {
		if r := recover(); r == nil {
			assert.Fail("Expected test to panic from bad interface conversion")
		}
	}()
	sort.Sort(lsp.Lines)
}

func TestLoglineSortByBootId(t *testing.T) {
	//t.Parallel()

	assert := assert.New(t)

	for i := 0; i < 10000; i++ {
		lsp := phonelab_backend.NewLoglineSortParams()

		rlg1 := new(RandomLoglineGenerator)
		rlg1.BootId = GenerateRandomBootId()
		rlg1.StartTimestamp = time.Now()
		rlg1.LastLogcatTimestamp = time.Now()
		rlg1.MaxDelayBetweenLoglines = 4 * time.Second

		line1 := GenerateRandomLogline(rlg1)

		rlg2 := new(RandomLoglineGenerator)
		rlg2.BootId = GenerateRandomBootId()
		rlg2.StartTimestamp = time.Now()
		rlg2.LastLogcatTimestamp = time.Now()
		rlg2.MaxDelayBetweenLoglines = 4 * time.Second

		line2 := GenerateRandomLogline(rlg2)

		lsp.Lines = append(lsp.Lines, phonelab_backend.ParseLoglineToSortInterface(line1))
		lsp.Lines = append(lsp.Lines, phonelab_backend.ParseLoglineToSortInterface(line2))

		// Now first confirm which bootID is supposed to appear before which
		var expected string

		switch strings.Compare(rlg1.BootId, rlg2.BootId) {
		case -1:
			expected = strings.TrimSpace(line1)
		case 0:
			assert.Fail("Same boot IDs across multiple RandomLoglineGenerators")
		case 1:
			expected = strings.TrimSpace(line2)
		}

		sort.Sort(lsp.Lines)

		got := strings.TrimSpace(lsp.Lines[0].String())

		assert.Equal(expected, got, "Sort by bootID failed")
	}
}

func TestLoglineSortWithNil(t *testing.T) {
	//t.Parallel()

	assert := assert.New(t)

	defer func() {
		// Fail on panic
		if r := recover(); r != nil {
			assert.Fail(fmt.Sprintf("Should not panic but received: %v", r))
		}
	}()

	rlg1 := new(RandomLoglineGenerator)
	rlg1.BootId = GenerateRandomBootId()
	rlg1.StartTimestamp = time.Now()
	rlg1.LastLogcatTimestamp = time.Now()
	rlg1.MaxDelayBetweenLoglines = 4 * time.Second

	line1 := GenerateRandomLogline(rlg1)

	lsp := phonelab_backend.NewLoglineSortParams()

	lsp.Lines = append(lsp.Lines, phonelab_backend.ParseLoglineToSortInterface(line1))
	lsp.Lines = append(lsp.Lines, phonelab_backend.ParseLoglineToSortInterface(""))

	sort.Sort(lsp.Lines)

	expected := strings.TrimSpace(line1)
	got := lsp.Lines[0].String()

	assert.Equal(expected, got, "Did not get expected line when nil was second")

	// Now, try with nil first
	lsp = phonelab_backend.NewLoglineSortParams()

	lsp.Lines = append(lsp.Lines, phonelab_backend.ParseLoglineToSortInterface(""))
	lsp.Lines = append(lsp.Lines, phonelab_backend.ParseLoglineToSortInterface(line1))

	sort.Sort(lsp.Lines)

	expected = strings.TrimSpace(line1)
	got = lsp.Lines[0].String()

	assert.Equal(expected, got, "Did not get expected line when nil was first")

}
