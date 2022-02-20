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
	LocalFileStorage = "LocalFile"
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

type FileDescriptor struct {
	FileStore      FileStorage
	FileName       string
	Type           string
	Encoding       string
	Header         bool
	FieldSeparator string
	Checksum       string
	ItemPrototype  interface{}
}

func (fd *FileDescriptor) String() string {
	return fmt.Sprintf("%s://%s", fd.FileStore, fd.FileName)
}

func (fd *FileDescriptor) ItemType() (reflect.Type, error) {
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
	Exists(fileName string) (ok bool, err error)
	Open(fileName, encoding string) (reader io.ReadCloser, err error)
	Create(fileName, encoding string) (writer io.WriteCloser, err error)
}

type FileItemReader interface {
	Open(fd FileDescriptor) (handle interface{}, err error)
	Close(handle interface{}) error
	ReadItem(handle interface{}) (interface{}, error)
	SkipTo(handle interface{}, pos int64) error
	Count(fd FileDescriptor) (int64, error)
}

type FileItemWriter interface {
	Open(fd FileDescriptor) (handle interface{}, err error)
	Close(handle interface{}) error
	WriteItem(handle interface{}, data interface{}) error
}

type FileMerger interface {
	Merge(src []FileDescriptor, dest FileDescriptor) (err error)
}

type FileSplitter interface {
	Split(src FileDescriptor, dest []FileDescriptor, strategy FileSplitStrategy) (err error)
}

type FileSplitStrategy interface {
	DecideDestNum(line string, dest []FileDescriptor) int
}

type MergeSplitter interface {
	FileMerger
	FileSplitter
}

type ChecksumVerifier interface {
	Verify(fd FileDescriptor) (bool, error)
}

type ChecksumFlusher interface {
	Checksum(fd FileDescriptor) error
}

type Checksumer interface {
	ChecksumVerifier
	ChecksumFlusher
}

func Count(fd FileDescriptor) (int64, error) {
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

func Merge(src []FileDescriptor, dest FileDescriptor) error {
	destFs := dest.FileStore
	writer, err := destFs.Create(dest.FileName, dest.Encoding)
	if err != nil {
		return err
	}
	defer writer.Close()
	bufWriter := bufio.NewWriter(writer)
	for i, srcFd := range src {
		err = copyFile(i, srcFd, bufWriter)
		if err != nil {
			return err
		}
	}
	return nil
}

func copyFile(idx int, srcFd FileDescriptor, writer *bufio.Writer) error {
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

func Split(srcFd FileDescriptor, destFds []FileDescriptor, strategy FileSplitStrategy) error {
	//open src file reader
	srcFs := srcFd.FileStore
	reader, err := srcFs.Open(srcFd.FileName, srcFd.Encoding)
	if err != nil {
		return err
	}
	defer reader.Close()
	bufReader := bufio.NewReader(reader)
	//open dest file writers
	writers := make([]io.Writer, 0)
	for _, destFd := range destFds {
		destFs := destFd.FileStore
		writer, err := destFs.Create(destFd.FileName, destFd.Encoding)
		if err != nil {
			return err
		}
		writers = append(writers, writer)
	}
	//write header
	if srcFd.Header && (srcFd.Type == TSV || srcFd.Type == CSV) {
		header, err := bufReader.ReadString('\n') //read first line
		if err != nil && err != io.EOF {
			return err
		}
		header = strings.TrimSpace(header)
		if header != "" {
			header = fmt.Sprintf("%s\n", header)
			for _, writer := range writers {
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
				_, er := writers[destNum].Write([]byte(line))
				if er != nil {
					return er
				}
			}
			break
		}
		destNum := strategy.DecideDestNum(line, destFds)
		_, er := writers[destNum].Write([]byte(line))
		if er != nil {
			return er
		}
	}
	return nil
}
