package gobatch

import "fmt"

type BatchError interface {
	Code() string
	Message() string
	Error() string
}

type batchErr struct {
	code string
	msg  string
}

func (err *batchErr) Code() string {
	return err.code
}

func (err *batchErr) Message() string {
	return err.msg
}

func (err *batchErr) Error() string {
	return fmt.Sprintf("batch err, code:%v, message:%v", err.code, err.msg)
}

func NewBatchError(code string, err interface{}) BatchError {
	if e, ok := err.(BatchError); ok {
		return e
	}
	return &batchErr{code: code, msg: fmt.Sprintf("%v", err)}
}

const (
	ErrCodeRetry       = "retry"
	ErrCodeStop        = "stop"
	ErrCodeConcurrency = "concurrency"
	ErrCodeDbFail      = "db_fail"
	ErrCodeGeneral     = "general"
)

var (
	RetryError      BatchError = &batchErr{code: ErrCodeRetry, msg: "should retry"}
	StopError       BatchError = &batchErr{code: ErrCodeStop, msg: "job stopping"}
	ConcurrentError BatchError = &batchErr{code: ErrCodeConcurrency, msg: "concurrency error"}
	DbError         BatchError = &batchErr{code: ErrCodeDbFail, msg: "db fail"}
)
