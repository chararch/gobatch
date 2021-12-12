package gobatch

import (
	"fmt"
	errors "github.com/pkg/errors"
	"reflect"
)

const (
	ItemReaderKeyList      = "gobatch.item.reader.key.list"
	ItemReaderCurrentIndex = "gobatch.item.reader.current.index"
	ItemReaderMaxIndex     = "gobatch.item.reader.max.index"
)

type ItemReader interface {
	ReadKeys() ([]interface{}, error)
	ReadItem(key interface{}) (interface{}, error)
}

type OpenCloser interface {
	Open(execution *StepExecution) error
	Close(execution *StepExecution) error
}

type defaultItemReader struct {
	itemReader ItemReader
}

func (reader defaultItemReader) Open(execution *StepExecution) BatchError {
	stepCtx := execution.StepContext
	executionCtx := execution.StepExecutionContext
	keyList := stepCtx.Get(ItemReaderKeyList)
	if keyList != nil {
		kind := reflect.TypeOf(keyList).Kind()
		if kind != reflect.Slice {
			return NewBatchError(ErrCodeGeneral, "the type of key list in context must be slice, but the actual is: %v", kind)
		}
	} else {
		keys, err := reader.itemReader.ReadKeys()
		if err != nil {
			return NewBatchError(ErrCodeGeneral, "ReadKeys() err", err)
		}
		stepCtx.Put(ItemReaderKeyList, keys)
		executionCtx.Put(ItemReaderCurrentIndex, 0)
		executionCtx.Put(ItemReaderMaxIndex, len(keys))
	}
	return nil
}

func (reader defaultItemReader) Read(chunkCtx *ChunkContext) (r interface{}, e BatchError) {
	defer func() {
		if err := recover(); err != nil {
			e = NewBatchError(ErrCodeGeneral, "panic on Read() in item reader, err:%v", err)
		}
	}()
	stepCtx := chunkCtx.StepExecution.StepContext
	executionCtx := chunkCtx.StepExecution.StepExecutionContext
	keyList := stepCtx.Get(ItemReaderKeyList)
	currentIndex, _ := executionCtx.GetInt(ItemReaderCurrentIndex)
	maxIndex, _ := executionCtx.GetInt(ItemReaderMaxIndex)
	if currentIndex < maxIndex {
		key := reflect.ValueOf(keyList).Index(currentIndex).Interface()
		var item interface{}
		var err error
		if reader.itemReader != nil {
			item, err = reader.itemReader.ReadItem(key)
		} else {
			err = errors.New("no ItemReader is specified")
		}
		if err != nil {
			return nil, NewBatchError(ErrCodeGeneral, "read item error", err)
		}
		executionCtx.Put(ItemReaderCurrentIndex, currentIndex+1)
		return item, nil
	}
	return nil, nil
}

func (reader defaultItemReader) Close(execution *StepExecution) BatchError {
	stepCtx := execution.StepContext
	executionCtx := execution.StepExecutionContext
	stepCtx.Remove(ItemReaderKeyList)
	executionCtx.Remove(ItemReaderCurrentIndex)
	executionCtx.Remove(ItemReaderMaxIndex)
	return nil
}

type defaultPartitioner struct {
	itemReader       ItemReader
	minPartitionSize uint
	maxPartitionSize uint
}

func (p *defaultPartitioner) Partition(execution *StepExecution, partitions uint) (subExecutions map[string]*StepExecution, e BatchError) {
	defer func() {
		if err := recover(); err != nil {
			e = NewBatchError(ErrCodeGeneral, "panic on Partition in defaultPartitioner, err:%v", err)
		}
	}()
	keys, err := p.itemReader.ReadKeys()
	if err != nil {
		return nil, NewBatchError(ErrCodeGeneral, "ReadKeys() err", err)
	}
	subExecutions = make(map[string]*StepExecution)
	count := len(keys)
	if count == 0 {
		return subExecutions, nil
	}
	partitionSize := uint(count) / partitions
	if partitionSize > p.maxPartitionSize {
		partitionSize = p.maxPartitionSize
	}
	if partitionSize < p.minPartitionSize {
		partitionSize = p.minPartitionSize
	}
	i := 0
	for start, end := 0, int(partitionSize); start < count; start, end = end, end+int(partitionSize) {
		if end > count {
			end = count
		}
		partitionName := fmt.Sprintf("%s:%04d", execution.StepName, i)
		partitionKeys := keys[start:end]
		subExecution := execution.deepCopy()
		subExecution.StepName = partitionName
		subExecution.StepContextId = 0
		subExecution.StepContext.Put(ItemReaderKeyList, partitionKeys)
		subExecution.StepExecutionContext.Put(ItemReaderCurrentIndex, 0)
		subExecution.StepExecutionContext.Put(ItemReaderMaxIndex, len(partitionKeys))
		subExecutions[partitionName] = subExecution
		i++
	}
	return subExecutions, nil
}

func (p *defaultPartitioner) GetPartitionNames(execution *StepExecution, partitions uint) []string {
	names := make([]string, 0)
	for i := uint(0); i < partitions; i++ {
		partitionName := fmt.Sprintf("%s:%04d", execution.StepName, i)
		names = append(names, partitionName)
	}
	return names
}