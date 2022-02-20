package file

import (
	"bufio"
	"github.com/pkg/errors"
	"io"
	"reflect"
	"strings"
)

type tsvReader struct {
	fd        FileDescriptor
	reader    io.ReadCloser
	bufReader *bufio.Reader
	metadata  *xsvMetadata
}
type tsvWriter struct {
	fd        FileDescriptor
	writer    io.WriteCloser
	bufWriter *bufio.Writer
	metadata  *xsvMetadata
}

type tsvFileItemReader struct {
}

func (r *tsvFileItemReader) Open(fd FileDescriptor) (interface{}, error) {
	if fd.Type != TSV {
		return nil, errors.New("file type doesn't match tsvFileItemReader")
	}
	fs := fd.FileStore
	reader, err := fs.Open(fd.FileName, fd.Encoding)
	if err != nil {
		return nil, err
	}
	bufReader := bufio.NewReader(reader)

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
	handle := &tsvReader{fd, reader, bufReader, metadata}
	if fd.Header {
		line, err := bufReader.ReadString('\n')
		if err != nil {
			reader.Close()
			return nil, err
		}
		sep := getFieldSeparator(fd)
		fields := strings.Split(line, sep)
		metadata.fileHeaders, metadata.fileHeaderMap = fields, slice2Map(fields)
	}
	return handle, nil
}
func (r *tsvFileItemReader) Close(handle interface{}) error {
	fdr := handle.(*tsvReader)
	return fdr.reader.Close()
}
func (r *tsvFileItemReader) ReadItem(handle interface{}) (interface{}, error) {
	fdr := handle.(*tsvReader)
	line, err := fdr.bufReader.ReadString('\n')
	if err != nil && err != io.EOF {
		return nil, err
	} else if err == io.EOF && line == "" {
		return nil, nil
	}
	sep := getFieldSeparator(fdr.fd)
	fields := strings.Split(line, sep)
	metadata := fdr.metadata
	var val reflect.Value
	if fdr.fd.Header {
		val, err = xsvUnmarshalByHeader(fields, metadata.fileHeaderMap, metadata.structType)
	} else {
		val, err = xsvUnmarshalByOrder(fields, metadata.structType)
	}
	if err != nil {
		return nil, err
	}
	return val.Interface(), nil
}
func getFieldSeparator(fd FileDescriptor) string {
	sep := fd.FieldSeparator
	if sep == "" {
		sep = "\t"
	}
	return sep
}
func (r *tsvFileItemReader) SkipTo(handle interface{}, pos int64) error {
	fdr := handle.(*tsvReader)
	for i := int64(0); i < pos; i++ {
		_, err := fdr.bufReader.ReadString('\n')
		if err != nil {
			return err
		}
	}
	return nil
}
func (r *tsvFileItemReader) Count(fd FileDescriptor) (int64, error) {
	return Count(fd)
}

type tsvFileItemWriter struct {
}

func (r *tsvFileItemWriter) Open(fd FileDescriptor) (interface{}, error) {
	if fd.Type != TSV {
		return nil, errors.New("file type doesn't match tsvFileItemWriter")
	}
	fs := fd.FileStore
	writer, err := fs.Create(fd.FileName, fd.Encoding)
	if err != nil {
		return nil, err
	}
	bufWriter := bufio.NewWriter(writer)

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
		_, err = bufWriter.WriteString(strings.Join(metadata.structFields, getFieldSeparator(fd)))
		if err != nil {
			return nil, err
		}
	}
	return &tsvWriter{fd, writer, bufWriter, metadata}, nil
}
func (r *tsvFileItemWriter) Close(handle interface{}) error {
	fdw := handle.(*tsvWriter)
	return fdw.writer.Close()
}
func (r *tsvFileItemWriter) WriteItem(handle interface{}, item interface{}) error {
	fdw := handle.(*tsvWriter)
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
	sep := getFieldSeparator(fdw.fd)
	_, err = fdw.bufWriter.WriteString(strings.Join(fields, sep))
	return err
}

type tsvFileMergeSplitter struct {
}
func (r *tsvFileMergeSplitter) Merge(src []FileDescriptor, dest FileDescriptor) error {
	err := Merge(src, dest)
	return err
}
func (r *tsvFileMergeSplitter) Split(src FileDescriptor, dest []FileDescriptor, strategy FileSplitStrategy) error {
	err := Split(src, dest, strategy)
	return err
}