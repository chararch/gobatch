package example2

import (
	"database/sql"
	"github.com/chararch/gobatch"
	"github.com/chararch/gobatch/file"
	"time"
)

var tradeFile = file.FileObjectModel{
	FileStore:     &file.LocalFileSystem{},
	FileName:      "res/trade.data",
	Type:          file.TSV,
	Encoding:      "utf-8",
	Header:        false,
	ItemPrototype: &Trade{},
}

var tradeFileExport = file.FileObjectModel{
	FileStore:     &file.LocalFileSystem{},
	FileName:      "res/{date,yyyyMMdd}/trade.csv",
	Type:          file.CSV,
	Encoding:      "utf-8",
	Checksum:      file.MD5,
	ItemPrototype: &Trade{},
}

var ftp = &file.FTPFileSystem{
	Hort:        "localhost",
	Port:        21,
	User:        "gobatch",
	Password:    "gobatch123",
	ConnTimeout: time.Second,
}

var copyFileToFtp = file.FileMove{
	FromFileName:  "res/{date,yyyyMMdd}/trade.csv",
	FromFileStore: &file.LocalFileSystem{},
	ToFileStore:   ftp,
	ToFileName:    "trade/{date,yyyyMMdd}/trade.csv",
}
var copyChecksumFileToFtp = file.FileMove{
	FromFileName:  "res/{date,yyyyMMdd}/trade.csv.md5",
	FromFileStore: &file.LocalFileSystem{},
	ToFileStore:   ftp,
	ToFileName:    "trade/{date,yyyyMMdd}/trade.csv.md5",
}

type tradeImporter struct {
	db *sql.DB
}

func (p *tradeImporter) Write(items []interface{}, chunkCtx *gobatch.ChunkContext) gobatch.BatchError {
	for _, item := range items {
		trade := item.(*Trade)
		_, err := p.db.Exec("INSERT INTO t_trade(trade_no, account_no, type, amount, terms, interest_rate, trade_time, status) values (?,?,?,?,?,?,?,?)",
			trade.TradeNo, trade.AccountNo, trade.Type, trade.Amount, trade.Terms, trade.InterestRate, trade.TradeTime, trade.Status)
		if err != nil {
			return gobatch.NewBatchError(gobatch.ErrCodeDbFail, "insert trade into db err", err)
		}
	}
	return nil
}
