package file

import (
	"fmt"
	"github.com/jlaffaye/ftp"
	"io"
	"net/textproto"
	"os"
	"strings"
	"time"
)

// LocalFileSystem a FileStorage implementation backed by local file system
type LocalFileSystem struct {
}

func (fs *LocalFileSystem) Name() string {
	return LocalFileStorage
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
	idx := strings.LastIndex(fileName, "/")
	if idx > 0 {
		path := fileName[0:idx]
		os.MkdirAll(path, os.ModePerm)
	}
	return os.Create(fileName)
}

// FTPFileSystem a FileStorage implementation backed by ftp
type FTPFileSystem struct {
	Hort        string
	Port        int
	User        string
	Password    string
	ConnTimeout time.Duration
}

func (fs *FTPFileSystem) Name() string {
	return FTPFileStorage
}
func (fs *FTPFileSystem) connect() (*ftp.ServerConn, error) {
	addr := fmt.Sprintf("%s:%d", fs.Hort, fs.Port)
	/*conn, err := net.DialTimeout("tcp", addr, fs.ConnTimeout)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetReadBuffer(1000*1000)
		tcpConn.SetWriteBuffer(1000*1000)
	}
	c, err := ftp.Dial(addr, ftp.DialWithNetConn(conn))*/
	c, err := ftp.DialTimeout(addr, fs.ConnTimeout)
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
	if err != nil {
		if c != nil {
			c.Quit()
		}
		return nil, err
	}
	r, err := c.Retr(fileName)
	if err != nil {
		c.Quit()
		return nil, err
	}
	return &FTPFileInStream{
		c: c,
		r: r,
	}, nil
}
func (fs *FTPFileSystem) Create(fileName, encoding string) (writer io.WriteCloser, err error) {
	c, err := fs.connect()
	if err != nil {
		if c != nil {
			defer c.Quit()
		}
		return nil, err
	}
	dirs := strings.Split(fileName, "/")
	for _, dir := range dirs[:len(dirs)-1] {
		if err = c.ChangeDir(dir); err != nil {
			if e, ok := err.(*textproto.Error); ok && e.Code != ftp.StatusFileUnavailable {
				return nil, err
			}
			if err = c.MakeDir(dir); err != nil {
				if e, ok := err.(*textproto.Error); ok && e.Code != ftp.StatusFileUnavailable {
					return nil, err
				}
			}
			if err = c.ChangeDir(dir); err != nil {
				return nil, err
			}
		}
	}
	name := dirs[len(dirs)-1]
	r, w := io.Pipe()
	ch := make(chan error)
	go func() {
		err = c.Stor(name, r)
		ch <- err
	}()
	return &FTPFileOutStream{
		c:  c,
		w:  w,
		ch: ch,
	}, nil
}

// FTPFileInStream represents a ftp file input stream
type FTPFileInStream struct {
	c *ftp.ServerConn
	r io.ReadCloser
}

func (fis *FTPFileInStream) Read(p []byte) (n int, err error) {
	return fis.r.Read(p)
}
func (fis *FTPFileInStream) Close() (err error) {
	defer func() {
		e := fis.c.Quit()
		if err == nil {
			err = e
		}
	}()
	err = fis.r.Close()
	return err
}

// FTPFileOutStream represents a ftp file output stream
type FTPFileOutStream struct {
	c  *ftp.ServerConn
	w  io.WriteCloser
	ch chan error
}

func (fis *FTPFileOutStream) Write(p []byte) (n int, err error) {
	return fis.w.Write(p)
}
func (fis *FTPFileOutStream) Close() (err error) {
	defer func() {
		err = <-fis.ch
		e := fis.c.Quit()
		if err == nil {
			err = e
		}
	}()
	err = fis.w.Close()
	return err
}
