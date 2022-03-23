package gobatch

import (
	"context"
	"fmt"
	"github.com/panjf2000/ants/v2"
)

type taskPool struct {
	pool *ants.Pool
}

func newTaskPool(size int) *taskPool {
	pool, _ := ants.NewPool(size)
	return &taskPool{
		pool: pool,
	}
}

// Future get result in future
type Future interface {
	Get() (interface{}, error)
}

type futureImpl struct {
	ch <-chan interface{}
}

func (f *futureImpl) Get() (interface{}, error) {
	result := <-f.ch
	err := <-f.ch
	if err == nil {
		return result, nil
	}
	e, ok := err.(error)
	if ok {
		return result, e
	}
	return result, fmt.Errorf("future get err:%v", err)
}

func (pool *taskPool) Submit(ctx context.Context, task func() (interface{}, error)) Future {
	result := make(chan interface{}, 2)
	err := pool.pool.Submit(func() {
		defer func() {
			if err := recover(); err != nil {
				//todo log
				result <- nil
				result <- fmt.Errorf("panic:%v", err)
				close(result)
			}
		}()
		val, err := task()
		result <- val
		result <- err
		close(result)
	})
	if err != nil {
		result <- nil
		result <- err
		close(result)
	}
	return &futureImpl{
		ch: result,
	}
}

func (pool *taskPool) Release() {
	pool.pool.Release()
}

func (pool *taskPool) SetMaxSize(size int) {
	pool.pool.Tune(size)
}
