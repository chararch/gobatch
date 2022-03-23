package example2

import (
	"database/sql"
	"fmt"
	"github.com/chararch/gobatch"
)

type tradeReader struct {
	db *sql.DB
}

func (h *tradeReader) Open(execution *gobatch.StepExecution) gobatch.BatchError {
	return nil
}
func (h *tradeReader) Close(execution *gobatch.StepExecution) gobatch.BatchError {
	return nil
}
func (h *tradeReader) ReadKeys() ([]interface{}, error) {
	rows, err := h.db.Query("select id from t_trade")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []interface{}
	var id int64
	for rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, nil
}
func (h *tradeReader) ReadItem(key interface{}) (interface{}, error) {
	id := int64(0)
	switch r := key.(type) {
	case int64:
		id = r
	case float64:
		id = int64(r)
	default:
		return nil, fmt.Errorf("key type error, type:%T, value:%v", key, key)
	}
	rows, err := h.db.Query("select trade_no, account_no, type, amount, terms, interest_rate, trade_time, status from t_trade where id = ?", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	trade := &Trade{}
	if rows.Next() {
		err = rows.Scan(&trade.TradeNo, &trade.AccountNo, &trade.Type, &trade.Amount, &trade.Terms, &trade.InterestRate, &trade.TradeTime, &trade.Status)
		if err != nil {
			return nil, err
		}
	}

	return trade, nil
}
