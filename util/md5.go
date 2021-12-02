package util

import (
	"crypto/md5"
	"fmt"
)

func MD5(str string) string {
	b := md5.Sum([]byte(str))
	return fmt.Sprintf("%x", b)
}