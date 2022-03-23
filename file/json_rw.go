package file

import (
	"bufio"
	"encoding/json"
	"github.com/pkg/errors"
	"io"
	"reflect"
)

type jsonReader struct {
	fd         FileObjectModel
	reader     io.ReadCloser
	bufReader  *bufio.Reader
	structType reflect.Type
}
type jsonWriter struct {
	fd        FileObjectModel
	writer    io.WriteCloser
	bufWriter *bufio.Writer
}

type jsonFileItemReader struct {
}

func (r *jsonFileItemReader) Open(fd FileObjectModel) (interface{}, error) {
	if fd.Type != JSON {
		return nil, errors.New("file type doesn't match jsonFileItemReader")
	}
	fs := fd.FileStore
	reader, err := fs.Open(fd.FileName, fd.Encoding)
	if err != nil {
		return nil, err
	}

	tp, err := fd.ItemType()
	if err != nil {
		return nil, err
	}
	return &jsonReader{fd, reader, bufio.NewReader(reader), tp}, nil
}
func (r *jsonFileItemReader) Close(handle interface{}) error {
	fdr := handle.(*jsonReader)
	return fdr.reader.Close()
}
func (r *jsonFileItemReader) ReadItem(handle interface{}) (interface{}, error) {
	fdr := handle.(*jsonReader)
	line, err := fdr.bufReader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, err
	} else if err == io.EOF && len(line) == 0 {
		return nil, nil
	}
	val := reflect.New(fdr.structType)
	err = json.Unmarshal(line, val)
	if err != nil {
		return nil, err
	}
	return val, nil
}
func (r *jsonFileItemReader) SkipTo(handle interface{}, pos int64) error {
	fdr := handle.(*jsonReader)
	for i := int64(0); i < pos; i++ {
		_, err := fdr.bufReader.ReadBytes('\n')
		if err != nil {
			return err
		}
	}
	return nil
}
func (r *jsonFileItemReader) Count(fd FileObjectModel) (int64, error) {
	return Count(fd)
}

type jsonFileItemWriter struct {
}

func (r *jsonFileItemWriter) Open(fd FileObjectModel) (interface{}, error) {
	if fd.Type != JSON {
		return nil, errors.New("file type doesn't match jsonFileItemWriter")
	}
	fs := fd.FileStore
	writer, err := fs.Create(fd.FileName, fd.Encoding)
	if err != nil {
		return nil, err
	}
	bufWriter := bufio.NewWriter(writer)
	return &jsonWriter{fd, writer, bufWriter}, nil
}
func (r *jsonFileItemWriter) Close(handle interface{}) error {
	fdw := handle.(*jsonWriter)
	defer func() {
		if err := fdw.writer.Close(); err != nil {
			//
		}
	}()
	return fdw.bufWriter.Flush()
}
func (r *jsonFileItemWriter) WriteItem(handle interface{}, item interface{}) error {
	fdw := handle.(*jsonWriter)
	buf, err := json.Marshal(item)
	if err != nil {
		return err
	}
	_, err = fdw.bufWriter.Write(buf)
	return err
}

type jsonFileMergeSplitter struct {
}

func (r *jsonFileMergeSplitter) Merge(src []FileObjectModel, dest FileObjectModel) error {
	err := Merge(src, dest)
	return err
}
func (r *jsonFileMergeSplitter) Split(src FileObjectModel, dest []FileObjectModel, strategy FileSplitStrategy) error {
	err := Split(src, dest, strategy)
	return err
}
