package util

import (
	"crypto/md5"
	"fmt"
)

// MD5 calculate md5 for a string
func MD5(str string) string {
	b := md5.Sum([]byte(str))
	return fmt.Sprintf("%x", b)
}
