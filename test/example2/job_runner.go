package example2

import (
	"context"
	"database/sql"
	"github.com/chararch/gobatch"
	"github.com/chararch/gobatch/util"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"time"
)

func openDB() *sql.DB {
	var sqlDb *sql.DB
	var err error
	sqlDb, err = sql.Open("mysql", "root:root123@tcp(127.0.0.1:3306)/example?charset=utf8&parseTime=true")
	if err != nil {
		log.Fatal(err)
	}
	return sqlDb
}

func removeJobData() {
	sqlDb := openDB()
	_, err := sqlDb.Exec("DELETE FROM t_trade")
	if err != nil {
		log.Fatal(err)
	}
	_, err = sqlDb.Exec("DELETE FROM t_repay_plan")
	if err != nil {
		log.Fatal(err)
	}
}

func buildAndRunJob() {
	sqlDb := openDB()
	gobatch.SetDB(sqlDb)
	gobatch.SetTransactionManager(gobatch.NewTransactionManager(sqlDb))

	step1 := gobatch.NewStep("import_trade").ReadFile(tradeFile).Writer(&tradeImporter{sqlDb}).Partitions(10).Build()
	step2 := gobatch.NewStep("gen_repay_plan").Reader(&tradeReader{sqlDb}).Handler(&repayPlanHandler{sqlDb}).Partitions(10).Build()
	step3 := gobatch.NewStep("stats").Handler(&statsHandler{sqlDb}).Build()
	step4 := gobatch.NewStep("export_trade").Reader(&tradeReader{sqlDb}).WriteFile(tradeFileExport).Partitions(10).Build()
	step5 := gobatch.NewStep("upload_file_to_ftp").CopyFile(copyFileToFtp, copyChecksumFileToFtp).Build()
	job := gobatch.NewJob("accounting_job").Step(step1, step2, step3, step4, step5).Build()

	gobatch.Register(job)

	params, _ := util.JsonString(map[string]interface{}{
		"date": time.Now().Format("2006-01-02"),
		"rand": time.Now().Nanosecond(),
	})
	gobatch.Start(context.Background(), job.Name(), params)

}
