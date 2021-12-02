package gobatch

type TransactionManager interface {
	BeginTx() (tx interface{}, err BatchError)
	Commit(tx interface{}) BatchError
	Rollback(tx interface{}) BatchError
}

var txManager TransactionManager

func SetTransactionManager(txMgr TransactionManager) {
	if txMgr == nil {
		panic("transaction manager must not be nil")
	}
	txManager = txMgr
}