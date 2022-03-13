package gobatch

import (
	"database/sql"
	"github.com/chararch/gobatch/internal/logs"
	"os"
)

//log
var logger logs.Logger = logs.NewLogger(os.Stdout, logs.Info)

func SetLogger(l logs.Logger) {
	logger = l
}

//task pool
const (
	DefaultJobPoolSize      = 10
	DefaultStepTaskPoolSize = 1000
)

var jobPool = newTaskPool(DefaultJobPoolSize)
var stepPool = newTaskPool(DefaultStepTaskPoolSize)
func SetMaxRunningJobs(size int) {
	jobPool.SetMaxSize(size)
}

func SetMaxRunningSteps(size int) {
	stepPool.SetMaxSize(size)
}

//db
var db *sql.DB

func SetDB(sqlDb *sql.DB) {
	if sqlDb == nil {
		panic("sqlDb must not be nil")
	}
	db = sqlDb
	if txManager == nil {
		txManager = &DefaultTxManager{sqlDb}
	}
}

//transaction manager
var txManager TransactionManager

func SetTransactionManager(txMgr TransactionManager) {
	if txMgr == nil {
		panic("transaction manager must not be nil")
	}
	txManager = txMgr
}