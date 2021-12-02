package util

import (
	"encoding/json"
)

func JsonString(v interface{}) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func ParseJson(jsonStr string, v interface{}) error {
	return json.Unmarshal([]byte(jsonStr), v)
}