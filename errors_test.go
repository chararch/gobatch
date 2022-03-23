package gobatch

import (
	"fmt"
	"testing"
)

func TestBatchErr_Format(t *testing.T) {
	fn := func() {
		batchErr := NewBatchError(ErrCodeGeneral, "new error")
		fmt.Printf("batchErr: %v\n", batchErr)
		fmt.Printf("batchErr detail: %+v\n", batchErr)
		stackTrace := batchErr.StackTrace()
		fmt.Printf("batchErr stack trace: %v\n", stackTrace)

		err := fmt.Errorf("some error raised from db")
		fmt.Printf("err:%v\n", err)
		batchErr2 := NewBatchError(ErrCodeDbFail, "wrap error", err)
		fmt.Printf("batchErr2: %v\n", batchErr2)
		fmt.Printf("batchErr2 detail: %+v\n", batchErr2)
		stackTrace2 := batchErr2.StackTrace()
		fmt.Printf("batchErr2 stack trace: %v\n", stackTrace2)

		batchErr3 := NewBatchError(ErrCodeDbFail, "wrap error:%v", err)
		fmt.Printf("batchErr3: %v\n", batchErr3)
		fmt.Printf("batchErr3 detail: %+v\n", batchErr3)
		stackTrace3 := batchErr3.StackTrace()
		fmt.Printf("batchErr3 stack trace: %v\n", stackTrace3)

	}
	fn()
}
