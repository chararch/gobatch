package example2

import (
	"database/sql"
	"fmt"
	"github.com/chararch/gobatch"
)

type statsHandler struct {
	db *sql.DB
}

func (ss *statsHandler) Handle(execution *gobatch.StepExecution) gobatch.BatchError {
	rows, err := ss.db.Query("select sum(principal) as total_principal, sum(interest) as total_interest from t_repay_plan")
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	var totalPrincipal float64
	var totalInterest float64
	if rows.Next() {
		err = rows.Scan(&totalPrincipal, &totalInterest)
		if err != nil {
			return gobatch.NewBatchError(gobatch.ErrCodeDbFail, "query t_repay_plan failed", err)
		}
	}
	fmt.Printf("totalPrincipal=%.2f, totalInterest=%.2f\n", totalPrincipal, totalInterest)
	return nil
}
