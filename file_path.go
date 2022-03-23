package gobatch

import (
	"fmt"
	"github.com/pkg/errors"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

//FilePath an abstract file path
type FilePath struct {
	NamePattern string
}

var paramRegexp = regexp.MustCompile("\\{[^\\}]+\\}")

//Format generate a real file path by formatting FilePath according to *StepExecution instance
func (f *FilePath) Format(execution *StepExecution) (string, error) {
	var err error
	factPath := paramRegexp.ReplaceAllStringFunc(f.NamePattern, func(s string) string {
		s = s[1 : len(s)-1]
		category, param, format := "", s, ""
		idx := strings.Index(s, ":")
		if idx > 0 {
			category, param = s[0:idx], s[idx+1:]
		}
		idx2 := strings.Index(param, ",")
		if idx2 > 0 {
			param, format = param[0:idx2], param[idx2+1:]
		}
		var paramVal interface{}
		if category == "" {
			if v, ok := execution.JobExecution.JobParams[param]; ok {
				paramVal = v
			} else if execution.StepContext.Exists(param) {
				paramVal = execution.StepContext.Get(param)
			} else if execution.JobExecution.JobContext.Exists(param) {
				paramVal = execution.JobExecution.JobContext.Get(param)
			} else {
				err = errors.Errorf("can not find param:%v", param)
			}
		} else if category == "job" {
			if execution.JobExecution.JobContext.Exists(param) {
				paramVal = execution.JobExecution.JobContext.Get(param)
			} else {
				err = errors.Errorf("can not find param:%v in JobExecution", param)
			}
		} else if category == "step" {
			if execution.StepContext.Exists(param) {
				paramVal = execution.StepContext.Get(param)
			} else {
				err = errors.Errorf("can not find param:%v in StepExecution", param)
			}
		} else {
			err = errors.Errorf("unsupported param category: %v", category)
		}
		var str string
		str, err = formatParam(paramVal, format)
		return str
	})
	if err != nil {
		return "", err
	}
	return factPath, nil
}

var dateFmtRegexp = regexp.MustCompile("yyyy|MM|dd|HH|mm|SS")

func formatParam(val interface{}, format string) (string, error) {
	if val == nil {
		return "", nil
	}
	if format == "" {
		return fmt.Sprintf("%v", val), nil
	} else if dateFmtRegexp.MatchString(format) {
		//format date
		format = strings.ReplaceAll(format, "yyyy", "2006")
		format = strings.ReplaceAll(format, "MM", "01")
		format = strings.ReplaceAll(format, "dd", "02")
		format = strings.ReplaceAll(format, "HH", "15")
		format = strings.ReplaceAll(format, "mm", "04")
		format = strings.ReplaceAll(format, "SS", "05")
		dt, err := parseDate(val)
		if err != nil {
			return "", err
		}
		return dt.Format(format), nil
	} else if idx := strings.Index(format, "#"); idx >= 0 {
		//format number
		digit := 0
		var err error
		if idx == 0 {
			digit, err = strconv.Atoi(format[1:])
		} else if idx > 0 {
			digit, err = strconv.Atoi(format[0:idx])
		}
		if err != nil {
			return "", errors.Errorf("unsupported format:%v", format)
		}
		val, err = parseInteger(val)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(fmt.Sprintf("%%%vd", digit), val), nil
	} else {
		return "", errors.Errorf("unsupported format:%v", format)
	}
}

func parseDate(val interface{}) (time.Time, error) {
	refVal := reflect.ValueOf(val)
	if refVal.Kind() == reflect.Struct && refVal.Type().String() == "time.Time" {
		return val.(time.Time), nil
	}
	if refVal.Kind() == reflect.String {
		strVal := val.(string)
		if len(strVal) == 8 {
			return time.ParseInLocation("20060102", strVal, time.Local)
		} else if len(strVal) == 10 {
			dt, err := time.ParseInLocation("2006-01-02", strVal, time.Local)
			return dt, err
		} else if len(strVal) == 19 {
			return time.ParseInLocation("2006-01-02 15:04:05", strVal, time.Local)
		}
	}
	return time.Time{}, errors.Errorf("can not parse to date:%v", val)
}

func parseInteger(val interface{}) (interface{}, error) {
	switch v := val.(type) {
	case int, int8, int32, int64, uint, uint8, uint32, uint64:
		return v, nil
	case string:
		return strconv.Atoi(v)
	}
	return -1, errors.Errorf("can not parse to integer:%v", val)
}
