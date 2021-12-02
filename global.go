package gobatch

import (
	"chararch/gobatch/internal/logs"
	"os"
)

var logger logs.Logger = logs.NewLogger(os.Stdout, logs.Info)

func SetLogger(l logs.Logger) {
	logger = l
}

