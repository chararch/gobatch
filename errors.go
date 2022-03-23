package gobatch

import (
	"fmt"
	"github.com/pkg/errors"
	"io"
)

//BatchError represent an error during GoBatch executing
type BatchError interface {
	//Code code of the error
	Code() string
	//Message readable message of the error
	Message() string
	//Error error interface
	Error() string
	//StackTrace goroutine stack trace
	StackTrace() string
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}
type causer interface {
	Cause() error
}

type batchError struct {
	code string
	msg  string
	err  error
}

func (err *batchError) Code() string {
	return err.code
}

func (err *batchError) Message() string {
	return err.msg
}

func (err *batchError) Error() string {
	if err.err.Error() == "" {
		return fmt.Sprintf("BatchError[%s]: %v", err.code, err.msg)
	}
	return fmt.Sprintf("BatchError[%s]: %v cause: %v", err.code, err.msg, err.err)
}

func (err *batchError) StackTrace() string {
	return fmt.Sprintf("%+v", err)
}

func (err *batchError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			if e, ok := err.err.(causer); ok {
				fmt.Fprintf(s, "%+v\n", e.Cause())
			}
			fmt.Fprintf(s, "BatchError[%s]: %v", err.code, err.msg)
			if st, ok := err.err.(stackTracer); ok {
				traces := st.StackTrace()
				if len(traces) > 0 {
					traces = traces[1:]
				}
				for _, t := range traces {
					fmt.Fprintf(s, "\n%+v", t)
				}
			}
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, err.Error())
	case 'q':
		fmt.Fprintf(s, "%q", err.Error())
	}
}

//NewBatchError new instance
func NewBatchError(code string, msg string, args ...interface{}) BatchError {
	var err error
	if len(args) > 0 {
		lastArg := args[len(args)-1]
		if e, ok := lastArg.(error); ok {
			args = args[0 : len(args)-1]
			if len(args) > 0 {
				msg = fmt.Sprintf(msg, args...)
			}
			err = errors.WithStack(e)
		} else {
			msg = fmt.Sprintf(msg, args...)
			err = errors.New("")
		}
	} else {
		err = errors.New("")
	}
	return &batchError{code: code, msg: msg, err: err}
}

const (
	//ErrCodeRetry an error indicating the caller should retry
	ErrCodeRetry = "retry"
	//ErrCodeStop an error indicating the job is to be stopped
	ErrCodeStop = "stop"
	//ErrCodeConcurrency an error indicating conflict modification
	ErrCodeConcurrency = "concurrency"
	//ErrCodeDbFail an error indicating database access failed
	ErrCodeDbFail = "db_fail"
	//ErrCodeGeneral general error
	ErrCodeGeneral = "general"
)
