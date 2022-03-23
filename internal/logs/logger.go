package logs

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"runtime"
	"time"
)

// Logger logger interface
type Logger interface {
	Debug(ctx context.Context, msg string, args ...interface{})
	Info(ctx context.Context, msg string, args ...interface{})
	Warn(ctx context.Context, msg string, args ...interface{})
	Error(ctx context.Context, msg string, args ...interface{})
}

// LogLevel log level
type LogLevel int

const (
	//Debug enable debug or above log output
	Debug LogLevel = 0
	//Info enable info or above log output
	Info LogLevel = 1
	//Warn enable warn or above log output
	Warn LogLevel = 2
	//Error enable error or above log output
	Error LogLevel = 3
)

func (ll LogLevel) String() string {
	if ll == Debug {
		return "DEBUG"
	} else if ll == Info {
		return "INFO"
	} else if ll == Warn {
		return "WARN"
	} else if ll == Error {
		return "ERROR"
	}
	return ""
}

type defaultLogger struct {
	writer   io.StringWriter
	logLevel LogLevel
}

//NewLogger init Logger instance
func NewLogger(writer io.StringWriter, logLevel LogLevel) *defaultLogger {
	return &defaultLogger{writer: writer, logLevel: logLevel}
}

func (l *defaultLogger) Debug(ctx context.Context, msg string, args ...interface{}) {
	if Debug >= l.logLevel {
		l.writer.WriteString(logBase(Debug) + fmt.Sprintf(msg, args...) + "\n")
	}
}

func (l *defaultLogger) Info(ctx context.Context, msg string, args ...interface{}) {
	if Info >= l.logLevel {
		l.writer.WriteString(logBase(Info) + fmt.Sprintf(msg, args...) + "\n")
	}
}

func (l *defaultLogger) Warn(ctx context.Context, msg string, args ...interface{}) {
	if Warn >= l.logLevel {
		l.writer.WriteString(logBase(Warn) + fmt.Sprintf(msg, args...) + "\n")
	}
}

func (l *defaultLogger) Error(ctx context.Context, msg string, args ...interface{}) {
	if Error >= l.logLevel {
		l.writer.WriteString(logBase(Error) + fmt.Sprintf(msg, args...) + "\n")
	}
}

var seperatorReg = regexp.MustCompile("[/\\\\]")

func fileLine() string {
	_, file, line, ok := runtime.Caller(3)
	if ok {
		idx := seperatorReg.FindAllStringIndex(file, -1)
		if len(idx) > 0 {
			file = file[idx[len(idx)-1][1]:]
		}
		return fmt.Sprintf("%s:%d", file, line)
	}
	return ""
}

func logBase(level LogLevel) string {
	return fmt.Sprintf("%v [%s] %s ", time.Now().Format("2006-01-02 15:04:05.000000"), level, fileLine())
}
