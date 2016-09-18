package phonelab_backend

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gurupras/gocommons"
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

func (l *Logline) String() string {
	return strings.TrimSpace(l.Line)
	//      return fmt.Sprintf("%v %v %v [%v] %v %v %v %v: %v",
	//              l.boot_id, l.datetime, l.LogcatToken, l.tracetime,
	//              l.pid, l.tid, l.level, l.tag, l.payload)
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

type Loglines []*Logline

func NewLoglineSortParams() *gocommons.SortParams {
	return &gocommons.SortParams{
		LineConvert: ParseLoglineToSortInterface,
		Lines:       make(gocommons.SortCollection, 0),
	}
}

func ParseLogline(line string) (logline *Logline, err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("line:", line)
			panic(fmt.Sprintf("%v", r))
		}
	}()

	names := PATTERN.SubexpNames()
	values_raw := PATTERN.FindAllStringSubmatch(line, -1)
	if values_raw == nil {
		//fmt.Fprintln(os.Stderr, "Line failed logcat pattern:", line)
		err = errors.New(fmt.Sprintf("Failed PATTERN.FindAllStringSubmatch(): \n%v\n", line))
		return
	}
	values := values_raw[0]

	kv_map := map[string]string{}
	for i, value := range values {
		kv_map[names[i]] = value
	}

	datetimeNanos, err := strconv.ParseInt(kv_map["datetime"][20:], 10, 64)
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to parse datetimeNanos: %v", err))
		return
	}
	// Convert values
	// Some datetimes are 9 digits instead of 6
	// TODO: Get rid of the last 3
	var datetime time.Time

	if len(kv_map["datetime"]) > 26 {
		kv_map["datetime"] = kv_map["datetime"][:26]
	}

	if datetime, err = strptime.Parse(kv_map["datetime"][:19], "%Y-%m-%d %H:%M:%S"); err != nil {
		err = errors.New(fmt.Sprintf("Failed to parse datetime: %v", err))
		return
	}

	logcat_token, err := strconv.ParseInt(kv_map["logcat_token"], 0, 64)
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to parse logcat_token: %v", err))
		return
	}

	// Cannot fail
	tracetime, _ := strconv.ParseFloat(kv_map["tracetime"], 64)

	pid, err := strconv.ParseInt(kv_map["pid"], 0, 32)
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to parse pid: %v", err))
		return
	}
	tid, err := strconv.ParseInt(kv_map["tid"], 0, 32)
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to parse tid: %v", err))
		return
	}

	logline = &Logline{line, kv_map["boot_id"], datetime, datetimeNanos, logcat_token,
		tracetime, int32(pid), int32(tid), kv_map["level"],
		kv_map["tag"], kv_map["payload"]}
	return
}

func ParseLoglineToSortInterface(line string) gocommons.SortInterface {
	if ll, _ := ParseLogline(line); ll != nil {
		return ll
	} else {
		return nil
	}

}

func SortLogs(inputPath string, outputPath string) (err error) {
	var (
		ofile         *gocommons.File
		ifile         *gocommons.File
		reader        *bufio.Scanner
		writer        gocommons.Writer
		line          string
		sortInterface gocommons.SortInterface
	)

	if ifile, err = gocommons.Open(inputPath, os.O_RDONLY, gocommons.GZ_TRUE); err != nil {
		err = errors.New(fmt.Sprintf("Failed to open input file for SortLogs(): %v", err))
		return
	}
	defer ifile.Close()

	if ofile, err = gocommons.Open(outputPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, gocommons.GZ_TRUE); err != nil {
		err = errors.New(fmt.Sprintf("Failed to open output file for SortLogs(): %v", err))
		return
	}
	defer ofile.Close()

	if writer, err = ofile.Writer(0); err != nil {
		err = errors.New(fmt.Sprintf("Failed to get writer to output file for SortLogs(): %v", err))
		return
	}
	defer writer.Close()
	defer writer.Flush()

	if reader, err = ifile.Reader(1024); err != nil {
		err = errors.New(fmt.Sprintf("Failed to get reader to input file (SortLogs()): %v", err))
		return
	}

	// TODO: Handle metadata

	// FIXME: Make this external sort maybe?
	sortParams := NewLoglineSortParams()

	reader.Split(bufio.ScanLines)

	for reader.Scan() {
		line = reader.Text()
		if sortInterface = sortParams.LineConvert(line); sortInterface == nil {
			//err = errors.New(fmt.Sprintf("Failed to parse line: \n%v\n", line))
			//return
			continue
		}
		sortParams.Lines = append(sortParams.Lines, sortInterface)
	}
	sort.Sort(sortParams.Lines)

	for idx, object := range sortParams.Lines {
		line := object.String()
		writer.Write([]byte(line))
		if idx < len(sortParams.Lines)-1 {
			writer.Write([]byte("\n"))
		}
		writer.Flush()
	}
	writer.Flush()
	writer.Close()
	return
}
