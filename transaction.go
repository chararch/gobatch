package gobatch

import (
	"database/sql"
)

type TransactionManager interface {
	BeginTx() (tx interface{}, err BatchError)
	Commit(tx interface{}) BatchError
	Rollback(tx interface{}) BatchError
}

type DefaultTxManager struct {
	db *sql.DB
}

func NewTransactionManager(db *sql.DB) TransactionManager {
	return &DefaultTxManager{
		db: db,
	}
}

func (tm *DefaultTxManager) BeginTx() (interface{}, BatchError) {
	tx, err := tm.db.Begin()
	if err != nil {
		return nil, NewBatchError(ErrCodeDbFail, "start transaction failed", err)
	}
	return tx, nil
}

func (tm *DefaultTxManager) Commit(tx interface{}) BatchError {
	tx1 := tx.(*sql.Tx)
	err := tx1.Commit()
	if err != nil {
		return NewBatchError(ErrCodeDbFail, "transaction commit failed", err)
	}
	return nil
}

func (tm *DefaultTxManager) Rollback(tx interface{}) BatchError {
	tx1 := tx.(*sql.Tx)
	err := tx1.Rollback()
	if err != nil {
		return NewBatchError(ErrCodeDbFail, "transaction rollback failed", err)
	}
	return nil
}