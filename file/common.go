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
	//LocalFileStorage name of local file storage
	LocalFileStorage = "File"
	//FTPFileStorage name of ftp file storage
	FTPFileStorage = "FTP"
)

const (
	//TSV a file type that have tab-seperated values as its content
	TSV = "tsv"
	//CSV a file type that have comma-seperated values as its content
	CSV = "csv"
	//JSON a file type that have json string as its content
	JSON = "json"
)

const (
	OKFlag = "OK"
	MD5    = "MD5"
	SHA1   = "SHA1"
	SHA256 = "SHA256"
	SHA512 = "SHA512"
)

//FileObjectModel represents mapping a file to/from a struct
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

//FileMove represents source and destination of a file movement
type FileMove struct {
	FromFileName  string
	FromFileStore FileStorage
	ToFileName    string
	ToFileStore   FileStorage
}

func (fd FileObjectModel) String() string {
	return fmt.Sprintf("%s://%s", fd.FileStore.Name(), fd.FileName)
}

//ItemType returns the reflect.Type of the item within a FileObjectModel
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

//FileStorage represents a file store media, from which we can read or write files
type FileStorage interface {
	//Name return name of the file store
	Name() string
	//Exists check if a file with the specified fileName exists
	Exists(fileName string) (ok bool, err error)
	//Open return a reader of the specified fileName, transform encoding if necessary
	Open(fileName, encoding string) (reader io.ReadCloser, err error)
	//Create return a writer of the specified fileName, transform encoding if necessary
	Create(fileName, encoding string) (writer io.WriteCloser, err error)
}

//FileItemReader read object from file according a FileObjectModel
type FileItemReader interface {
	//Open open file of the FileObjectModel, returns a handle for later read
	Open(fd FileObjectModel) (handle interface{}, err error)
	//Close open the file handle
	Close(handle interface{}) error
	//ReadItem read an item from the file handle
	ReadItem(handle interface{}) (interface{}, error)
	//SkipTo skip to the pos'th record in the file
	SkipTo(handle interface{}, pos int64) error
	//Count return number of items contained by the file
	Count(fd FileObjectModel) (int64, error)
}

//FileItemWriter write object to file according to a FileObjectModel
type FileItemWriter interface {
	//Open open file of the FileObjectModel, returns a handle for later write
	Open(fd FileObjectModel) (handle interface{}, err error)
	//Close open the file handle
	Close(handle interface{}) error
	//WriteItem write an item to the file
	WriteItem(handle interface{}, data interface{}) error
}

//FileMerger merge multiple files into a simple one
type FileMerger interface {
	//Merge merge multiple files into a simple one
	Merge(src []FileObjectModel, dest FileObjectModel) (err error)
}

//FileSplitter split a src file into multiple dest ones
type FileSplitter interface {
	//Split split a src file into multiple destinations, decide the destination of every record according to a FileSplitStrategy
	Split(src FileObjectModel, dest []FileObjectModel, strategy FileSplitStrategy) (err error)
}

//FileSplitStrategy decide the destination of every record
type FileSplitStrategy interface {
	//DecideDestIndex decide the destination of every record, return the index of destination file in the dest array
	DecideDestIndex(line string, dest []FileObjectModel) int
}

//MergeSplitter the combination of FileMerger and FileSplitter
type MergeSplitter interface {
	FileMerger
	FileSplitter
}

//ChecksumVerifier verify a data file according to the corresponding check file
type ChecksumVerifier interface {
	//Verify verify a data file according to the corresponding check file
	Verify(fd FileObjectModel) (bool, error)
}

//ChecksumFlusher write a check file for the corresponding data file
type ChecksumFlusher interface {
	//Checksum generate checksum and write a check file for the corresponding data file
	Checksum(fd FileObjectModel) error
}

//Checksumer the combination of ChecksumVerifier and ChecksumFlusher
type Checksumer interface {
	ChecksumVerifier
	ChecksumFlusher
}

//Count the default implementation to count number of items contained by the file
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

//Merge the default implementation to merge multiple files into a simple one
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

//Split the default implementation to split a file into multiple destinations
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
				destIdx := strategy.DecideDestIndex(line, destFds)
				_, er := bufWriters[destIdx].Write([]byte(line))
				if er != nil {
					return er
				}
			}
			break
		}
		destIdx := strategy.DecideDestIndex(line, destFds)
		_, er := bufWriters[destIdx].Write([]byte(line))
		if er != nil {
			return er
		}
	}
	return nil
}
