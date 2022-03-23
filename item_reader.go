package gobatch

import (
	"context"
	"fmt"
	errors "github.com/pkg/errors"
	"reflect"
)

const (
	//ItemReaderKeyList the key of keyList in StepContext
	ItemReaderKeyList = "gobatch.ItemReader.key.list"
	//ItemReaderCurrentIndex the key of current offset of step's keyList in StepContext
	ItemReaderCurrentIndex = "gobatch.ItemReader.current.index"
	//ItemReaderMaxIndex the key of max index of step's keyList in StepContext
	ItemReaderMaxIndex = "gobatch.ItemReader.max.index"
)

// ItemReader is for loading large amount of data from a datasource like database, used in a chunk step.
// When the step executing, it first loads all data keys by calling ReadKeys() once, then load full data by key one by one in every chunk.
type ItemReader interface {
	//ReadKeys read all keys of some kind of data
	ReadKeys() ([]interface{}, error)
	//ReadItem read value by one key from ReadKeys result
	ReadItem(key interface{}) (interface{}, error)
}

type defaultChunkReader struct {
	itemReader ItemReader
}

func (reader *defaultChunkReader) Open(execution *StepExecution) BatchError {
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

func (reader *defaultChunkReader) Read(chunkCtx *ChunkContext) (r interface{}, e BatchError) {
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

func (reader *defaultChunkReader) Close(execution *StepExecution) BatchError {
	return nil
}

func (reader *defaultChunkReader) GetPartitioner(minPartitionSize, maxPartitionSize uint) Partitioner {
	return &defaultPartitioner{
		itemReader:       reader.itemReader,
		minPartitionSize: minPartitionSize,
		maxPartitionSize: maxPartitionSize,
	}
}

type defaultPartitioner struct {
	itemReader       ItemReader
	minPartitionSize uint
	maxPartitionSize uint
}

func (p *defaultPartitioner) Partition(execution *StepExecution, partitions uint) (subExecutions []*StepExecution, e BatchError) {
	defer func() {
		if err := recover(); err != nil {
			e = NewBatchError(ErrCodeGeneral, "panic on Partition in defaultPartitioner, err:%v", err)
		}
	}()
	keys, err := p.itemReader.ReadKeys()
	if err != nil {
		return nil, NewBatchError(ErrCodeGeneral, "ReadKeys() err", err)
	}
	subExecutions = make([]*StepExecution, 0)
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
		subExecutions = append(subExecutions, subExecution)
		i++
	}
	logger.Info(context.Background(), "partition step:%v, total count:%v, partitions:%v, partitionSize:%v, subExecutions:%v", execution.StepName, count, partitions, partitionSize, len(subExecutions))
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
