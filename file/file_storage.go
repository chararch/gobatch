package file

import (
	"fmt"
	"github.com/jlaffaye/ftp"
	"io"
	"net/textproto"
	"os"
	"time"
)

type LocalFileSystem struct {
}

func (fs *LocalFileSystem) Exists(fileName string) (bool, error) {
	_, err := os.Stat(fileName)
	if err != nil && os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
func (fs *LocalFileSystem) Open(fileName, encoding string) (io.ReadCloser, error) {
	file, err := os.Open(fileName)
	return file, err
}
func (fs *LocalFileSystem) Create(fileName, encoding string) (io.WriteCloser, error) {
	return os.Create(fileName)
}

type FTPFileSystem struct {
	Hort        string
	Port        int
	User        string
	Password    string
	ConnTimeout time.Duration
}

func (fs *FTPFileSystem) connect() (*ftp.ServerConn, error) {
	c, err := ftp.DialTimeout(fmt.Sprintf("%s:%d", fs.Hort, fs.Port), fs.ConnTimeout)
	if err != nil {
		return nil, err
	}

	err = c.Login(fs.User, fs.Password)
	return c, err
}

func (fs *FTPFileSystem) Exists(fileName string) (bool, error) {
	c, err := fs.connect()
	if c != nil {
		defer c.Quit()
	}
	if err != nil {
		return false, err
	}

	_, err = c.FileSize(fileName)
	if err == nil {
		return true, nil
	}
	if err != nil {
		if e, ok := err.(*textproto.Error); ok && e.Code == ftp.StatusFileUnavailable {
			return false, nil
		}
	}

	return false, err
}
func (fs *FTPFileSystem) Open(fileName, encoding string) (io.ReadCloser, error) {
	c, err := fs.connect()
	if c != nil {
		defer c.Quit()
	}
	if err != nil {
		return nil, err
	}

	r, err := c.Retr(fileName)
	return r, err
}
func (fs *FTPFileSystem) Create(fileName, encoding string) (writer io.WriteCloser, err error) {
	c, err := fs.connect()
	if c != nil {
		defer c.Quit()
	}
	if err != nil {
		return nil, err
	}
	r, w := io.Pipe()
	err = c.Stor(fileName, r)
	if err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}
