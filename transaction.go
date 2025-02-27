package gobatch

import (
	"database/sql"
)
// TransactionManager defines an interface for managing database transactions.
// It provides methods to begin, commit and rollback transactions during chunk processing.
type TransactionManager interface {
	BeginTx() (tx interface{}, err BatchError)
	Commit(tx interface{}) BatchError
	Rollback(tx interface{}) BatchError
}

// DefaultTxManager implements the TransactionManager interface using a standard SQL database connection
type DefaultTxManager struct {
	db *sql.DB
}

// NewTransactionManager creates and returns a new instance of TransactionManager
// with the provided database connection
func NewTransactionManager(db *sql.DB) TransactionManager {
	return &DefaultTxManager{
		db: db,
	}
}

// BeginTx initiates a new database transaction and returns the transaction object
func (tm *DefaultTxManager) BeginTx() (interface{}, BatchError) {
	tx, err := tm.db.Begin()
	if err != nil {
		return nil, NewBatchError(ErrCodeDbFail, "start transaction failed", err)
	}
	return tx, nil
}

// Commit finalizes the given transaction by persisting all changes to the database
func (tm *DefaultTxManager) Commit(tx interface{}) BatchError {
	tx1 := tx.(*sql.Tx)
	err := tx1.Commit()
	if err != nil {
		return NewBatchError(ErrCodeDbFail, "transaction commit failed", err)
	}
	return nil
}

// Rollback aborts the given transaction and discards all changes made within it
func (tm *DefaultTxManager) Rollback(tx interface{}) BatchError {
	tx1 := tx.(*sql.Tx)
	err := tx1.Rollback()
	if err != nil {
		return NewBatchError(ErrCodeDbFail, "transaction rollback failed", err)
	}
	return nil
}
