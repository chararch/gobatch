package gobatch

import (
	"chararch/gobatch/file"
	"fmt"
	"strings"
)

const (
	FileItemWriterHandleKey   = "gobatch.FileItemWriter.handle"
	FileItemWriterFileNameKey = "gobatch.FileItemWriter.fileName"
)

type fileWriter struct {
	fd        file.FileDescriptor
	writer    file.FileItemWriter
	checkumer file.ChecksumFlusher
	merger    file.FileMerger
}

func (w *fileWriter) Open(execution *StepExecution) BatchError {
	stepName := execution.StepName
	//get actual file name
	fd := w.fd                            //copy fd
	fp := &FilePath{fd.FileName}
	fileName, err := fp.Format(execution)
	if err != nil {
		return NewBatchError(ErrCodeGeneral, "get real file path:%v err:%v", fd.FileName, err)
	}
	fd.FileName = fileName
	if strings.Index(stepName, ":") > 0 { //may be a partitioned step
		fd.FileName = fmt.Sprintf("%v.%v", fd.FileName, strings.ReplaceAll(stepName, ":", "."))
	}
	handle, e := w.writer.Open(fd)
	if e != nil {
		return NewBatchError(ErrCodeGeneral, "open file writer[%v] err:%v", fd.FileName, e)
	}
	execution.StepExecutionContext.Put(FileItemWriterHandleKey, handle)
	execution.StepExecutionContext.Put(FileItemWriterFileNameKey, fd.FileName)
	return nil
}
func (w *fileWriter) Write(items []interface{}, chunkCtx *ChunkContext) BatchError {
	executionCtx := chunkCtx.StepExecution.StepExecutionContext
	handle := executionCtx.Get(FileItemWriterHandleKey)
	fileName := executionCtx.Get(FileItemWriterFileNameKey)
	for _, item := range items {
		e := w.writer.WriteItem(handle, item)
		if e != nil {
			return NewBatchError(ErrCodeGeneral, "write item to file[%v] err:%v", fileName, e)
		}
	}
	return nil
}
func (w *fileWriter) Close(execution *StepExecution) BatchError {
	executionCtx := execution.StepExecutionContext
	handle := executionCtx.Get(FileItemWriterHandleKey)
	fileName := executionCtx.Get(FileItemWriterFileNameKey)
	executionCtx.Remove(FileItemWriterHandleKey)
	e := w.writer.Close(handle)
	if e != nil {
		return NewBatchError(ErrCodeGeneral, "close file writer[%v] err:%v", fileName, e)
	}
	//generate file checksum
	if w.fd.Checksum != "" {
		fd := w.fd
		fd.FileName = fileName.(string)
		checksumer := file.GetChecksumer(fd.Checksum)
		if checksumer != nil {
			err := checksumer.Checksum(fd)
			if err != nil {
				return NewBatchError(ErrCodeGeneral, "generate file checksum[%v] err:%v", fd, err)
			}
		}
	}
	return nil
}

func (w *fileWriter) Aggregate(execution *StepExecution, subExecutions []*StepExecution) BatchError {
	if w.merger != nil {
		subFiles := make([]file.FileDescriptor, 0)
		for _, subExecution := range subExecutions {
			fileName := subExecution.StepExecutionContext.Get(FileItemWriterFileNameKey)
			fd := w.fd
			fd.FileName = fileName.(string)
			subFiles = append(subFiles, fd)
		}
		fd := w.fd
		fp := &FilePath{fd.FileName}
		fileName, err := fp.Format(execution)
		if err != nil {
			return NewBatchError(ErrCodeGeneral, "get real file path:%v err:%v", fd.FileName, err)
		}
		fd.FileName = fileName
		err = w.merger.Merge(subFiles, fd)
		if err != nil {
			return NewBatchError(ErrCodeGeneral, "aggregate file[%v] err:%v", fd.FileName, err)
		}
		//generate file checksum
		if fd.Checksum != "" {
			checksumer := file.GetChecksumer(fd.Checksum)
			if checksumer != nil {
				err = checksumer.Checksum(fd)
				if err != nil {
					return NewBatchError(ErrCodeGeneral, "generate file checksum[%v] err:%v", fd, err)
				}
			}
		}
	}
	return nil
}