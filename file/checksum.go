package file

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"io"
	"strings"
)

type OKFlagChecksumer struct {
}

func (ch *OKFlagChecksumer) Verify(fd FileDescriptor) (bool, error) {
	fs := fd.FileStore
	ok, err := fs.Exists(fd.FileName)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	okFile := fd.FileName + ".ok"
	ok, err = fs.Exists(okFile)
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	dotIdx := strings.LastIndex(fd.FileName, ".")
	if dotIdx > 0 {
		okFile = fd.FileName[0:dotIdx] + ".ok"
		ok, err = fs.Exists(okFile)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func (ch *OKFlagChecksumer) Checksum(fd FileDescriptor) error {
	fs := fd.FileStore
	okFile := fd.FileName + ".ok"
	w, err := fs.Create(okFile, fd.Encoding)
	if err != nil {
		return err
	}
	return w.Close()
}

type MD5Checksumer struct {
}
func (ch *MD5Checksumer) Verify(fd FileDescriptor) (bool, error) {
	return verify(fd, MD5, md5.New())
}
func (ch *MD5Checksumer) Checksum(fd FileDescriptor) error {
	return checksum(fd, MD5, md5.New())
}

type SHA1Checksumer struct {
}
func (ch *SHA1Checksumer) Verify(fd FileDescriptor) (bool, error) {
	return verify(fd, SHA1, sha1.New())
}
func (ch *SHA1Checksumer) Checksum(fd FileDescriptor) error {
	return checksum(fd, SHA1, sha1.New())
}

type SHA256Checksumer struct {
}
func (ch *SHA256Checksumer) Verify(fd FileDescriptor) (bool, error) {
	return verify(fd, SHA256, sha256.New())
}
func (ch *SHA256Checksumer) Checksum(fd FileDescriptor) error {
	return checksum(fd, SHA256, sha256.New())
}

type SHA512Checksumer struct {
}
func (ch *SHA512Checksumer) Verify(fd FileDescriptor) (bool, error) {
	return verify(fd, SHA512, sha512.New())
}
func (ch *SHA512Checksumer) Checksum(fd FileDescriptor) error {
	return checksum(fd, SHA512, sha512.New())
}

func verify(fd FileDescriptor, alg string, digest hash.Hash) (bool, error) {
	fs := fd.FileStore
	ok, err := fs.Exists(fd.FileName)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	checkFile := fmt.Sprintf("%s.%s", fd.FileName, strings.ToLower(alg))
	ok, err = fs.Exists(checkFile)
	if err != nil {
		return false, err
	}
	if !ok {
		checkFile = fmt.Sprintf("%s.%s", fd.FileName, strings.ToUpper(alg))
		ok, err = fs.Exists(checkFile)
		if err != nil {
			return false, err
		}
		if !ok {
			dotIdx := strings.LastIndex(fd.FileName, ".")
			if dotIdx > 0 {
				checkFile = fmt.Sprintf("%s.%s", fd.FileName[0:dotIdx], strings.ToLower(alg))
				ok, err = fs.Exists(checkFile)
				if err != nil {
					return false, err
				}
				if !ok {
					checkFile = fmt.Sprintf("%s.%s", fd.FileName[0:dotIdx], strings.ToUpper(alg))
					ok, err = fs.Exists(checkFile)
					if err != nil {
						return false, err
					}
				}
			}
		}
	}
	if !ok {
		return false, nil
	}
	//read checksum from check file
	checkReader, err := fs.Open(checkFile, fd.Encoding)
	if err != nil {
		return false, err
	}
	defer checkReader.Close()
	buf, err := io.ReadAll(checkReader)
	if err != nil {
		return false, err
	}
	hashVal := strings.TrimSpace(string(buf))

	//calc checksum from data file
	reader, err := fs.Open(fd.FileName, fd.Encoding)
	if err != nil {
		return false, err
	}
	defer reader.Close()
	_, err = io.Copy(digest, reader)
	if err != nil {
		return false, err
	}
	fileHash := fmt.Sprintf("%x", digest.Sum(nil))

	return hashVal == fileHash, nil
}
func checksum(fd FileDescriptor, alg string, digest hash.Hash) error {
	fs := fd.FileStore
	//calc checksum from file
	reader, err := fs.Open(fd.FileName, fd.Encoding)
	if err != nil {
		return err
	}
	defer reader.Close()
	_, err = io.Copy(digest, reader)
	if err != nil {
		return err
	}
	fileHash := fmt.Sprintf("%x", digest.Sum(nil))

	// writer checksum to check file
	checkFile := fmt.Sprintf("%s.%s", fd.FileName, strings.ToLower(alg))
	w, err := fs.Create(checkFile, fd.Encoding)
	if err != nil {
		return err
	}
	w.Close()
	_, err = w.Write([]byte(fileHash))
	return err
}