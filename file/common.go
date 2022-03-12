package file

import (
	"bufio"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"reflect"
	"strings"
)

const (
	LocalFileStorage = "File"
	FTPFileStorage   = "FTP"
)

const (
	TSV  = "tsv"
	CSV  = "csv"
	JSON = "json"
)

const (
	OKFlag = "OK"
	MD5    = "MD5"
	SHA1   = "SHA1"
	SHA256 = "SHA256"
	SHA512 = "SHA512"
)

type FileObjectModel struct {
	FileStore      FileStorage
	FileName       string
	Type           string
	Encoding       string
	Header         bool
	FieldSeparator string
	Checksum       string
	ItemPrototype  interface{}
}

type FileMove struct {
	FromFileName  string
	FromFileStore FileStorage
	ToFileName    string
	ToFileStore   FileStorage
}

func (fd FileObjectModel) String() string {
	return fmt.Sprintf("%s://%s", fd.FileStore.Name(), fd.FileName)
}

func (fd FileObjectModel) ItemType() (reflect.Type, error) {
	prot := fd.ItemPrototype
	tp := reflect.TypeOf(prot)
	if tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}
	if tp.Kind() != reflect.Struct {
		return nil, errors.New("the underlying type of ItemPrototype is not struct for " + fd.String())
	}
	return tp, nil
}

type FileStorage interface {
	Name() string
	Exists(fileName string) (ok bool, err error)
	Open(fileName, encoding string) (reader io.ReadCloser, err error)
	Create(fileName, encoding string) (writer io.WriteCloser, err error)
}

type FileItemReader interface {
	Open(fd FileObjectModel) (handle interface{}, err error)
	Close(handle interface{}) error
	ReadItem(handle interface{}) (interface{}, error)
	SkipTo(handle interface{}, pos int64) error
	Count(fd FileObjectModel) (int64, error)
}

type FileItemWriter interface {
	Open(fd FileObjectModel) (handle interface{}, err error)
	Close(handle interface{}) error
	WriteItem(handle interface{}, data interface{}) error
}

type FileMerger interface {
	Merge(src []FileObjectModel, dest FileObjectModel) (err error)
}

type FileSplitter interface {
	Split(src FileObjectModel, dest []FileObjectModel, strategy FileSplitStrategy) (err error)
}

type FileSplitStrategy interface {
	DecideDestNum(line string, dest []FileObjectModel) int
}

type MergeSplitter interface {
	FileMerger
	FileSplitter
}

type ChecksumVerifier interface {
	Verify(fd FileObjectModel) (bool, error)
}

type ChecksumFlusher interface {
	Checksum(fd FileObjectModel) error
}

type Checksumer interface {
	ChecksumVerifier
	ChecksumFlusher
}

func Count(fd FileObjectModel) (int64, error) {
	fs := fd.FileStore
	reader, err := fs.Open(fd.FileName, fd.Encoding)
	if err != nil {
		return -1, err
	}
	defer reader.Close()
	bufReader := bufio.NewReader(reader)
	count := int64(0)
	if fd.Header && (fd.Type == TSV || fd.Type == CSV) {
		_, err := bufReader.ReadString('\n') //read first line
		if err != nil && err != io.EOF {
			return -1, err
		}
		if err == io.EOF {
			return count, nil
		}
	}
	for {
		line, err := bufReader.ReadString('\n')
		if err != nil && err != io.EOF {
			return -1, err
		}
		if err == io.EOF {
			if line != "" {
				count++
			}
			return count, nil
		}
		count++
	}
}

func Merge(src []FileObjectModel, dest FileObjectModel) error {
	destFs := dest.FileStore
	writer, err := destFs.Create(dest.FileName, dest.Encoding)
	if err != nil {
		return err
	}
	bufWriter := bufio.NewWriter(writer)
	defer func() {
		if er := bufWriter.Flush(); er != nil {
			if err == nil {
				err = er
			}
		}
		if er := writer.Close(); er != nil {
			if err == nil {
				err = er
			}
		}
	}()
	for i, srcFd := range src {
		err = copyFile(i, srcFd, bufWriter)
		if err != nil {
			return err
		}
	}
	return nil
}

func copyFile(idx int, srcFd FileObjectModel, writer *bufio.Writer) error {
	srcFs := srcFd.FileStore
	reader, err := srcFs.Open(srcFd.FileName, srcFd.Encoding)
	if err != nil {
		return err
	}
	defer reader.Close()
	bufReader := bufio.NewReader(reader)

	if srcFd.Header && (srcFd.Type == TSV || srcFd.Type == CSV) {
		header, err := bufReader.ReadString('\n') //read first line
		if err != nil && err != io.EOF {
			return err
		}
		if idx == 0 {
			header = strings.TrimSpace(header)
			if header != "" {
				header = fmt.Sprintf("%s\n", header)
				_, er := writer.Write([]byte(header))
				if er != nil {
					return er
				}
			}
		}
		if err == io.EOF {
			return nil
		}
	}
	for {
		line, err := bufReader.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			if line != "" {
				if line[len(line)-1] != '\n' {
					line = line + "\n"
				}
				_, err := writer.WriteString(line)
				if err != nil {
					return err
				}
			}
			break
		}
		_, err = writer.WriteString(line)
		if err != nil {
			return err
		}
	}
	return nil
}

func Split(srcFd FileObjectModel, destFds []FileObjectModel, strategy FileSplitStrategy) error {
	//open src file reader
	srcFs := srcFd.FileStore
	reader, err := srcFs.Open(srcFd.FileName, srcFd.Encoding)
	if err != nil {
		return err
	}
	defer reader.Close()
	bufReader := bufio.NewReader(reader)
	//open dest file writers
	writers := make([]io.WriteCloser, 0)
	bufWriters := make([]*bufio.Writer, 0)
	for _, destFd := range destFds {
		destFs := destFd.FileStore
		writer, err := destFs.Create(destFd.FileName, destFd.Encoding)
		if err != nil {
			return err
		}
		writers = append(writers, writer)
		bufWriters = append(bufWriters, bufio.NewWriter(writer))
	}
	//close writers
	defer func() {
		for i, writer := range writers {
			bufWriters[i].Flush()
			if err := writer.Close(); err != nil {
				//
			}
		}
	}()
	//write header
	if srcFd.Header && (srcFd.Type == TSV || srcFd.Type == CSV) {
		header, err := bufReader.ReadString('\n') //read first line
		if err != nil && err != io.EOF {
			return err
		}
		header = strings.TrimSpace(header)
		if header != "" {
			header = fmt.Sprintf("%s\n", header)
			for _, bufWriter := range bufWriters {
				_, er := bufWriter.Write([]byte(header))
				if er != nil {
					return er
				}
			}
		}
		if err == io.EOF {
			return nil
		}
	}
	//write file content
	for {
		line, err := bufReader.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			if line != "" {
				if line[len(line)-1] != '\n' {
					line = line + "\n"
				}
				destNum := strategy.DecideDestNum(line, destFds)
				_, er := bufWriters[destNum].Write([]byte(line))
				if er != nil {
					return er
				}
			}
			break
		}
		destNum := strategy.DecideDestNum(line, destFds)
		_, er := bufWriters[destNum].Write([]byte(line))
		if er != nil {
			return er
		}
	}
	return nil
}