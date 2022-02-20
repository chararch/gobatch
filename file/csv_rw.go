package file

import (
	"bufio"
	"encoding/csv"
	"github.com/pkg/errors"
	"io"
	"reflect"
)

type csvReader struct {
	fd       FileDescriptor
	reader   io.ReadCloser
	cReader  *csv.Reader
	metadata *xsvMetadata
}
type csvWriter struct {
	fd       FileDescriptor
	writer   io.WriteCloser
	cWriter  *csv.Writer
	metadata *xsvMetadata
}

type csvFileItemReader struct {
}
func (r *csvFileItemReader) Open(fd FileDescriptor) (interface{}, error) {
	if fd.Type != CSV {
		return nil, errors.New("file type doesn't match csvFileItemReader")
	}
	fs := fd.FileStore
	reader, err := fs.Open(fd.FileName, fd.Encoding)
	if err != nil {
		return nil, err
	}
	cReader := csv.NewReader(bufio.NewReader(reader))

	prot := fd.ItemPrototype
	tp := reflect.TypeOf(prot)
	if tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}
	if tp.Kind() != reflect.Struct {
		reader.Close()
		return nil, errors.New("the underlying type of ItemPrototype is not struct for " + fd.FileName)
	}
	metadata, err := getMetadata(tp)
	if err != nil {
		reader.Close()
		return nil, err
	}
	handle := &csvReader{fd, reader, cReader, metadata}
	if fd.Header {
		record, err := cReader.Read()
		if err != nil {
			reader.Close()
			return nil, err
		}
		metadata.fileHeaders, metadata.fileHeaderMap = record, slice2Map(record)
	}
	return handle, nil
}
func (r *csvFileItemReader) Close(handle interface{}) error {
	fdr := handle.(*csvReader)
	return fdr.reader.Close()
}
func (r *csvFileItemReader) ReadItem(handle interface{}) (interface{}, error) {
	fdr := handle.(*csvReader)
	record, err := fdr.cReader.Read()
	if err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	metadata := fdr.metadata
	var val reflect.Value
	if fdr.fd.Header {
		val, err = xsvUnmarshalByHeader(record, metadata.fileHeaderMap, metadata.structType)
	} else {
		val, err = xsvUnmarshalByOrder(record, metadata.structType)
	}
	if err != nil {
		return nil, err
	}
	return val.Interface(), nil
}
func (r *csvFileItemReader) SkipTo(handle interface{}, pos int64) error {
	fdr := handle.(*csvReader)
	for i := int64(0); i < pos; i++ {
		_, err := fdr.cReader.Read()
		if err != nil {
			return err
		}
	}
	return nil
}
func (r *csvFileItemReader) Count(fd FileDescriptor) (int64, error) {
	return Count(fd)
}

type csvFileItemWriter struct {
}
func (r *csvFileItemWriter) Open(fd FileDescriptor) (interface{}, error) {
	if fd.Type != CSV {
		return nil, errors.New("file type doesn't match csvFileItemWriter")
	}
	fs := fd.FileStore
	writer, err := fs.Create(fd.FileName, fd.Encoding)
	if err != nil {
		return nil, err
	}
	cWriter := csv.NewWriter(bufio.NewWriter(writer))
	prot := fd.ItemPrototype
	tp := reflect.TypeOf(prot)
	if tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}
	if tp.Kind() != reflect.Struct {
		return nil, errors.New("the underlying type of ItemPrototype is not struct for " + fd.FileName)
	}
	metadata, err := getMetadata(tp)
	if err != nil {
		return nil, err
	}
	if fd.Header {
		err = cWriter.Write(metadata.structFields)
		if err != nil {
			//close writer
			return nil, err
		}
	}
	return &csvWriter{fd, writer, cWriter, metadata}, nil
}
func (r *csvFileItemWriter) Close(handle interface{}) error {
	fdw := handle.(*csvWriter)
	return fdw.writer.Close()
}
func (r *csvFileItemWriter) WriteItem(handle interface{}, item interface{}) error {
	fdw := handle.(*csvWriter)
	var fields []string
	var err error
	if fdw.fd.Header {
		fields, err = xsvMarshalByHeader(item, fdw.metadata)
	} else {
		fields, err = xsvMarshalByOrder(item, fdw.metadata)
	}
	if err != nil {
		return err
	}
	err = fdw.cWriter.Write(fields)
	return err
}

type csvFileMergeSplitter struct {
}
func (r *csvFileMergeSplitter) Merge(src []FileDescriptor, dest FileDescriptor) error {
	err := Merge(src, dest)
	return err
}
func (r *csvFileMergeSplitter) Split(src FileDescriptor, dest []FileDescriptor, strategy FileSplitStrategy) error {
	err := Split(src, dest, strategy)
	return err
}