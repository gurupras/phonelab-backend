package phonelab_backend_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/gurupras/phonelab_backend"
	"github.com/jehiah/go-strftime"
	"github.com/stretchr/testify/assert"
)

func GenerateRandomLogline(bootId string, timeOffset int64, tokenNumber int64) string {
	tokens := make([]string, 0)

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

	tag := fmt.Sprintf("DummyTag->%s", GenerateRandomString(rand.Intn(32)))
	tokens = append(tokens, tag)

	payload := GenerateRandomString(32 + rand.Intn(256))
	tokens = append(tokens, payload)

	return strings.Join(tokens, "\t") + "\n"
}

func GenerateRandomString(length int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789!@#$%^&*()_+[]{};:'\",<.>/?\\|	 "
	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
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
