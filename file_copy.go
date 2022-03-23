package gobatch

import (
	"context"
	"github.com/chararch/gobatch/file"
	"io"
)

type fileCopyHandler struct {
	filesToMove []file.FileMove
}

func (handler *fileCopyHandler) Handle(execution *StepExecution) BatchError {
	for _, fm := range handler.filesToMove {
		ffp := &FilePath{fm.FromFileName}
		fromFileName, err := ffp.Format(execution)
		if err != nil {
			return NewBatchError(ErrCodeGeneral, "get real file path:%v err", fm.FromFileName, err)
		}
		tfp := &FilePath{fm.ToFileName}
		toFileName, err := tfp.Format(execution)
		if err != nil {
			return NewBatchError(ErrCodeGeneral, "get real file path:%v err", fm.ToFileName, err)
		}

		//open from-file
		ffs := fm.FromFileStore
		reader, err := ffs.Open(fromFileName, "utf-8")
		if err != nil {
			return NewBatchError(ErrCodeGeneral, "open from file:%v err", fromFileName, err)
		}

		//create to-file
		tfs := fm.ToFileStore
		writer, err := tfs.Create(toFileName, "utf-8")
		if err != nil {
			if er := reader.Close(); er != nil {
				logger.Error(context.Background(), "close file reader:%v error", fromFileName, er)
			}
			return NewBatchError(ErrCodeGeneral, "open to file:%v err", toFileName, err)
		}

		_, err = io.Copy(writer, reader)

		if er := reader.Close(); er != nil {
			logger.Error(context.Background(), "close file writer:%v error", toFileName, er)
		}
		if er := writer.Close(); er != nil {
			logger.Error(context.Background(), "close file writer:%v error", toFileName, er)
		}
		if err != nil {
			return NewBatchError(ErrCodeGeneral, "copy file: %v -> %v error", fromFileName, toFileName, err)
		}
	}
	return nil

}
