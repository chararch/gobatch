package gobatch

import (
	"database/sql"
	"github.com/chararch/gobatch/internal/logs"
	"os"
)

//log
var logger logs.Logger = logs.NewLogger(os.Stdout, logs.Info)

//SetLogger set a logger instance for GoBatch
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

//SetMaxRunningJobs set max number of parallel jobs for GoBatch
func SetMaxRunningJobs(size int) {
	jobPool.SetMaxSize(size)
}

//SetMaxRunningSteps set max number of parallel steps for GoBatch
func SetMaxRunningSteps(size int) {
	stepPool.SetMaxSize(size)
}

//db
var db *sql.DB

//SetDB register a *sql.DB instance for GoBatch
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

//SetTransactionManager register a TransactionManager instance for GoBatch
func SetTransactionManager(txMgr TransactionManager) {
	if txMgr == nil {
		panic("transaction manager must not be nil")
	}
	txManager = txMgr
}
