package example2

import "time"

// Trade trade model
type Trade struct {
	TradeNo      string    `order:"0" header:"trade_no"`
	AccountNo    string    `order:"1" header:"account_no"`
	Type         string    `order:"2" header:"type"`
	Amount       float64   `order:"3" header:"amount"`
	Terms        int       `order:"4" header:"terms"`
	InterestRate float64   `order:"5" header:"interest_rate"`
	TradeTime    time.Time `order:"7" header:"trade_time"`
	Status       string    `order:"6" header:"status"`
}

// RepayPlan repay plan model
type RepayPlan struct {
	Id         int64
	AccountNo  string
	LoanNo     string
	Term       int
	Principal  float64
	Interest   float64
	InitDate   time.Time
	RepayDate  time.Time
	RepayState string
	CreateTime time.Time
	UpdateTime time.Time
}
