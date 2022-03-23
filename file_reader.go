package gobatch

import (
	"context"
	"fmt"
	"github.com/chararch/gobatch/file"
)

const (
	fileItemReaderHandleKey    = "gobatch.FileItemReader.handle"
	fileItemReaderFileNameKey  = "gobatch.FileItemWriter.fileName"
	fileItemReaderCurrentIndex = "gobatch.FileItemReader.current.index"
	fileItemReaderStart        = "gobatch.FileItemReader.start"
	fileItemReaderEnd          = "gobatch.FileItemReader.end"
)

type fileReader struct {
	fd       file.FileObjectModel
	reader   file.FileItemReader
	verifier file.ChecksumVerifier
}

func (r *fileReader) Open(execution *StepExecution) BatchError {
	//get actual file name
	fd := r.fd //copy fd
	fp := &FilePath{fd.FileName}
	fileName, err := fp.Format(execution)
	if err != nil {
		return NewBatchError(ErrCodeGeneral, "get real file path:%v err", fd.FileName, err)
	}
	fd.FileName = fileName
	//verify checksum
	if fd.Checksum != "" {
		checksumer := file.GetChecksumer(fd.Checksum)
		if checksumer != nil {
			ok, err := checksumer.Verify(fd)
			if err != nil || !ok {
				return NewBatchError(ErrCodeGeneral, "verify file checksum:%v, ok:%v err", fd, ok, err)
			}
		}
	}
	//read file
	handle, e := r.reader.Open(fd)
	if e != nil {
		return NewBatchError(ErrCodeGeneral, "open file reader:%v err", fd, e)
	}
	execution.StepExecutionContext.Put(fileItemReaderHandleKey, handle)
	execution.StepExecutionContext.Put(fileItemReaderFileNameKey, fd.FileName)
	executionCtx := execution.StepExecutionContext
	currentIndex, _ := executionCtx.GetInt64(fileItemReaderCurrentIndex)
	err = r.reader.SkipTo(handle, currentIndex)
	if err != nil {
		return NewBatchError(ErrCodeGeneral, "skip to file item:%v pos:%v err", fd, currentIndex, err)
	}
	return nil
}

func (r *fileReader) Read(chunkCtx *ChunkContext) (interface{}, BatchError) {
	stepCtx := chunkCtx.StepExecution.StepContext
	executionCtx := chunkCtx.StepExecution.StepExecutionContext
	endPos, _ := stepCtx.GetInt64(fileItemReaderEnd)
	currentIndex, _ := executionCtx.GetInt64(fileItemReaderCurrentIndex)
	handle := executionCtx.Get(fileItemReaderHandleKey)
	fileName := executionCtx.Get(fileItemReaderFileNameKey)
	if currentIndex < endPos {
		item, e := r.reader.ReadItem(handle)
		if e != nil {
			return nil, NewBatchError(ErrCodeGeneral, "read item from file:%v err", fileName, e)
		}
		executionCtx.Put(fileItemReaderCurrentIndex, currentIndex+1)
		return item, nil
	}
	return nil, nil
}

func (r *fileReader) Close(execution *StepExecution) BatchError {
	executionCtx := execution.StepExecutionContext
	handle := executionCtx.Get(fileItemReaderHandleKey)
	fileName := executionCtx.Get(fileItemReaderFileNameKey)
	executionCtx.Remove(fileItemReaderHandleKey)
	e := r.reader.Close(handle)
	if e != nil {
		return NewBatchError(ErrCodeGeneral, "close file reader:%v err", fileName, e)
	}
	return nil
}

func (r *fileReader) GetPartitioner(minPartitionSize, maxPartitionSize uint) Partitioner {
	return &filePartitioner{
		fd:               r.fd,
		reader:           r.reader,
		minPartitionSize: minPartitionSize,
		maxPartitionSize: maxPartitionSize,
	}
}

type filePartitioner struct {
	fd               file.FileObjectModel
	reader           file.FileItemReader
	minPartitionSize uint
	maxPartitionSize uint
}

func (p *filePartitioner) Partition(execution *StepExecution, partitions uint) (subExecutions []*StepExecution, e BatchError) {
	defer func() {
		if err := recover(); err != nil {
			e = NewBatchError(ErrCodeGeneral, "panic on Partition in filePartitioner, err", err)
		}
	}()
	// get actual file name
	fd := p.fd //copy fd
	fp := &FilePath{fd.FileName}
	fileName, err := fp.Format(execution)
	if err != nil {
		return nil, NewBatchError(ErrCodeGeneral, "get real file path:%v err", fd.FileName, err)
	}
	fd.FileName = fileName
	//verify checksum
	if fd.Checksum != "" {
		checksumer := file.GetChecksumer(fd.Checksum)
		if checksumer != nil {
			ok, err := checksumer.Verify(fd)
			if err != nil || !ok {
				return nil, NewBatchError(ErrCodeGeneral, "verify file checksum:%v, ok:%v err", fd, ok, err)
			}
		}
	}
	//read file
	count, err := p.reader.Count(fd)
	if err != nil {
		return nil, NewBatchError(ErrCodeGeneral, "Count() err", err)
	}
	subExecutions = make([]*StepExecution, 0)
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
	i := uint(0)
	for start, end := int64(0), int64(partitionSize); start < count; start, end = end, end+int64(partitionSize) {
		if end > count {
			end = count
		}
		partitionName := genPartitionStepName(execution, i)
		subExecution := execution.deepCopy()
		subExecution.StepName = partitionName
		subExecution.StepContextId = 0
		subExecution.StepContext.Put(fileItemReaderStart, start)
		subExecution.StepContext.Put(fileItemReaderEnd, end)
		subExecution.StepExecutionContext.Put(fileItemReaderCurrentIndex, start)
		subExecutions = append(subExecutions, subExecution)
		i++
	}
	logger.Info(context.Background(), "partition step:%v, total count:%v, partitions:%v, partitionSize:%v, subExecutions:%v", execution.StepName, count, partitions, partitionSize, len(subExecutions))
	return subExecutions, nil
}

func genPartitionStepName(execution *StepExecution, i uint) string {
	partitionName := fmt.Sprintf("%s:%04d", execution.StepName, i)
	return partitionName
}

func (p *filePartitioner) GetPartitionNames(execution *StepExecution, partitions uint) []string {
	names := make([]string, 0)
	for i := uint(0); i < partitions; i++ {
		partitionName := genPartitionStepName(execution, i)
		names = append(names, partitionName)
	}
	return names
}
