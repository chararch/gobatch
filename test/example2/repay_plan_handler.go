package example2

import (
	"database/sql"
	"github.com/chararch/gobatch"
	"time"
)

type repayPlanHandler struct {
	//TradeReader
	db *sql.DB
}

func (h *repayPlanHandler) Process(item interface{}, chunkCtx *gobatch.ChunkContext) (interface{}, gobatch.BatchError) {
	trade := item.(*Trade)
	plans := make([]*RepayPlan, 0)
	restPrincipal := trade.Amount
	for i := 1; i <= trade.Terms; i++ {
		principal := restPrincipal / float64(trade.Terms-i+1)
		interest := restPrincipal * trade.InterestRate / 12
		repayPlan := &RepayPlan{
			AccountNo:  trade.AccountNo,
			LoanNo:     trade.TradeNo,
			Term:       i,
			Principal:  principal,
			Interest:   interest,
			InitDate:   time.Now(),
			RepayDate:  time.Now().AddDate(0, 1, 0),
			RepayState: "",
			CreateTime: time.Now(),
			UpdateTime: time.Now(),
		}
		plans = append(plans, repayPlan)
		restPrincipal -= principal
	}
	return plans, nil
}

func (h *repayPlanHandler) Write(items []interface{}, chunkCtx *gobatch.ChunkContext) gobatch.BatchError {
	for _, item := range items {
		plans := item.([]*RepayPlan)
		for _, plan := range plans {
			_, err := h.db.Exec("INSERT INTO t_repay_plan(account_no, loan_no, term, principal, interest, init_date, repay_date, repay_state) values (?,?,?,?,?,?,?,?)",
				plan.AccountNo, plan.LoanNo, plan.Term, plan.Principal, plan.Interest, plan.InitDate, plan.RepayDate, plan.RepayState)
			if err != nil {
				return gobatch.NewBatchError(gobatch.ErrCodeDbFail, "insert t_repay_plan failed", err)
			}
		}
	}
	return nil
}
