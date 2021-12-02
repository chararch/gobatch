package gobatch

import (
	"context"
	"fmt"
	"github.com/bmizerany/assert"
	"testing"
)

func TestFutureImpl_Get(t *testing.T) {
	ctx := context.Background()
	fu := jobPool.Submit(ctx, func() (interface{}, error) {
		return "ok", nil
	})
	val, err := fu.Get()
	assert.Equal(t, "ok", val)
	assert.Equal(t, nil, err)

	fu = jobPool.Submit(ctx, func() (interface{}, error) {
		var m []string
		return m[0], nil
	})
	val, err = fu.Get()
	assert.Equal(t, nil, val)
	assert.NotEqual(t, nil, err)
	fmt.Printf("val:%v err:%v\n", val, err)

	jobPool.Release()
	fu = jobPool.Submit(ctx, func() (interface{}, error) {
		return "ok", nil
	})
	val, err = fu.Get()
	assert.Equal(t, nil, val)
	assert.NotEqual(t, nil, err)
	fmt.Printf("val:%v err:%v\n", val, err)
}
