package phonelab_backend

import (
	"regexp"
	"strconv"
	"time"

	"github.com/pbnjay/strptime"
)

var PATTERN = regexp.MustCompile(`` +
	`\s*(?P<line>` +
	`\s*(?P<boot_id>[a-z0-9\-]{36})` +
	`\s*(?P<datetime>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)` +
	`\s*(?P<logcat_token>\d+)` +
	`\s*\[\s*(?P<tracetime>\d+\.\d+)\]` +
	`\s*(?P<pid>\d+)` +
	`\s*(?P<tid>\d+)` +
	`\s*(?P<level>[A-Z]+)` +
	`\s*(?P<tag>\S+)\s*:` +
	`\s*(?P<payload>.*)` +
	`)`)

// This is useful for testing. Make sure that this matches whatever the final
// output format is.
var PHONELAB_PATTERN = regexp.MustCompile(`` +
	`(?P<line>` +
	`(?P<deviceid>[a-z0-9]+)` +
	`\s+(?P<logcat_timestamp>\d+)` +
	`\s+(?P<logcat_timestamp_sub>\d+(\.\d+)?)` +
	`\s+(?P<boot_id>[a-z0-9\-]{36})` +
	`\s+(?P<logcat_token>\d+)` +
	`\s+(?P<tracetime>\d+\.\d+)` +
	`\s+(?P<datetime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)` +
	`\s+(?P<pid>\d+)` +
	`\s+(?P<tid>\d+)` +
	`\s+(?P<level>[A-Z]+)` +
	`\s+(?P<tag>\S+)` +
	`\s+(?P<payload>.*)` +
	`)`)

func ParseLogline(line string) *Logline {
	var err error

	names := PATTERN.SubexpNames()
	values_raw := PATTERN.FindAllStringSubmatch(line, -1)
	if values_raw == nil {
		//fmt.Fprintln(os.Stderr, "Line failed logcat pattern:", line)
		return nil
	}
	values := values_raw[0]

	kv_map := map[string]string{}
	for i, value := range values {
		kv_map[names[i]] = value
	}

	datetimeNanos, err := strconv.ParseInt(kv_map["datetime"][20:], 0, 64)
	if err != nil {
		return nil
	}
	// Convert values
	// Some datetimes are 9 digits instead of 6
	// TODO: Get rid of the last 3
	datetime, err := strptime.Parse(kv_map["datetime"][:26], "%Y-%m-%d %H:%M:%S.%f")
	if err != nil {
		return nil
	}
	logcat_token, err := strconv.ParseInt(kv_map["logcat_token"], 0, 64)
	if err != nil {
		return nil
	}

	// Cannot fail
	tracetime, _ := strconv.ParseFloat(kv_map["tracetime"], 64)

	pid, err := strconv.ParseInt(kv_map["pid"], 0, 32)
	if err != nil {
		return nil
	}
	tid, err := strconv.ParseInt(kv_map["tid"], 0, 32)
	if err != nil {
		return nil
	}

	result := Logline{line, kv_map["boot_id"], datetime, datetimeNanos, logcat_token,
		tracetime, int32(pid), int32(tid), kv_map["level"],
		kv_map["tag"], kv_map["payload"]}
	return &result
}

type Logline struct {
	Line          string
	BootId        string
	Datetime      time.Time
	DatetimeNanos int64
	LogcatToken   int64
	TraceTime     float64
	Pid           int32
	Tid           int32
	Level         string
	Tag           string
	Payload       string
}

type Loglines []*Logline

// Commented out till implementation is done
/*
func ParseLoglineConvert(line string) gocommons.SortInterface {
	if ll := ParseLogline(line); ll != nil {
		return ll
	} else {
		return nil
	}

}

var (
	LoglineSortParams = gocommons.SortParams{LineConvert: ParseLoglineConvert, Lines: make(gocommons.SortCollection, 0)}
)

func (l *Logline) String() string {
	return l.Line
}

func (l *Logline) Less(s gocommons.SortInterface) (ret bool, err error) {
	var o *Logline
	var ok bool
	if s != nil {
		if o, ok = s.(*Logline); !ok {
			err = errors.New(fmt.Sprintf("Failed to convert from SortInterface to *Logline:", reflect.TypeOf(s)))
			ret = false
			goto out
		}
	}
	if l != nil && o != nil {
		bootComparison := strings.Compare(l.BootId, o.BootId)
		if bootComparison == -1 {
			ret = true
		} else if bootComparison == 1 {
			ret = false
		} else {
			// Same boot ID..compare the other fields
			if l.LogcatToken == o.LogcatToken {
				ret = l.TraceTime < o.TraceTime
			} else {
				ret = l.LogcatToken < o.LogcatToken
			}
		}
	} else if l != nil {
		ret = true
	} else {
		ret = false
	}
out:
	return
}
*/
