package phonelab_backend

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/jehiah/go-strftime"
	"github.com/stretchr/testify/assert"
)

func GenerateRandomLogline(bootId string, timeOffset int64, tokenNumber int64) string {
	tokens := make([]string, 0)

	tokens = append(tokens, bootId)

	timeNow := time.Now()
	tokens = append(tokens, fmt.Sprintf("%s.%d", strftime.Format("%Y-%m-%d %H:%M:%S", timeNow), timeNow.Nanosecond()))

	tokens = append(tokens, fmt.Sprintf("%v", tokenNumber))

	offset := (timeNow.UnixNano() - timeOffset) / 1000
	tokens = append(tokens, fmt.Sprintf("[%f]", offset/1e6))

	tokens = append(tokens, fmt.Sprintf("%v", rand.Int31n(32768)))
	tokens = append(tokens, fmt.Sprintf("%v", rand.Int31n(32768)))

	levels := []string{"V", "D", "I", "W", "E", "C", "WTF"}
	tokens = append(tokens, levels[rand.Intn(len(levels))])

	tag := fmt.Sprintf("DummyTag->%s%v", GenerateRandomString(rand.Intn(32)))
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
	line := "6b793913-7cd9-477a-bbfa-62f07fbac87b 2016-04-21 09:59:01.199025638 11553177 [29981.752359]   202   203 D Kernel-Trace:      kworker/1:1-21588 [001] ...2 29981.751893: phonelab_periodic_ctx_switch_info: cpu=1 pid=7641 tgid=7613 nice=0 comm=Binder_1 utime=0 stime=0 rtime=158906 bg_utime=0 bg_stime=0 bg_rtime=0 s_run=0 s_int=2 s_unint=0 s_oth=0 log_idx=79981"

	logline := ParseLogline(line)

	payload := "kworker/1:1-21588 [001] ...2 29981.751893: phonelab_periodic_ctx_switch_info: cpu=1 pid=7641 tgid=7613 nice=0 comm=Binder_1 utime=0 stime=0 rtime=158906 bg_utime=0 bg_stime=0 bg_rtime=0 s_run=0 s_int=2 s_unint=0 s_oth=0 log_idx=79981"

	assert.NotEqual(t, nil, logline, "Failed to parse logline")
	assert.Equal(t, "6b793913-7cd9-477a-bbfa-62f07fbac87b", logline.BootId, "BootId was not parsed properly")
	assert.Equal(t, "2016-04-21 09:59:01", strftime.Format("%Y-%m-%d %H:%M:%S", logline.Datetime), "Datetime was not parsed properly")
	assert.Equal(t, int64(199025638), logline.DatetimeNanos, "DatetimeNanos was not parsed properly")
	assert.Equal(t, int64(11553177), logline.LogcatToken, "LogcatToken was not parsed properly")
	assert.Equal(t, 29981.752359, logline.TraceTime, "TraceTime was not parsed properly")
	assert.Equal(t, int32(202), logline.Pid, "Pid was not parsed properly")
	assert.Equal(t, int32(203), logline.Tid, "Tid was not parsed properly")
	assert.Equal(t, "D", logline.Level, "Level was not parsed properly")
	assert.Equal(t, "Kernel-Trace", logline.Tag, "Tag was not parsed properly")
	assert.Equal(t, payload, logline.Payload, "Payload was not parsed properly")
}