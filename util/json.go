package util

import (
	"encoding/json"
)

// JsonString generate json string for an object
func JsonString(v interface{}) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ParseJson parse json string to an object
func ParseJson(jsonStr string, v interface{}) error {
	return json.Unmarshal([]byte(jsonStr), v)
}
