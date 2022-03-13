package logs

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"runtime"
	"time"
)


type Logger interface {
	Debug(ctx context.Context, msg string, args ...interface{})
	Info(ctx context.Context, msg string, args ...interface{})
	Warn(ctx context.Context, msg string, args ...interface{})
	Error(ctx context.Context, msg string, args ...interface{})
}

type LogLevel int

const (
	Debug LogLevel = 0
	Info           = 1
	Warn           = 2
	Error          = 3
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
