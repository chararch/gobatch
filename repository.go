package gobatch

import (
	"bytes"
	"chararch/gobatch/status"
	"chararch/gobatch/util"
	"context"
	"database/sql"
	"github.com/pkg/errors"
	"time"
)

var db *sql.DB

func SetDB(sqlDb *sql.DB) {
	if sqlDb == nil {
		panic("sqlDb must not be nil")
	}
	db = sqlDb
}

type batchJobInstance struct {
	JobInstanceId int64
	JobName       string
	JobKey        string
	JobParams     string
	CreateTime    time.Time
}

type batchJobExecution struct {
	JobExecutionId int64
	JobInstanceId  int64
	JobName        string
	CreateTime     time.Time
	StartTime      time.Time
	EndTime        time.Time
	Status         string
	ExitCode       string
	ExitMessage    string
	LastUpdated    time.Time
	Version        int64
}

type batchJobContext struct {
	ContextId     int64
	JobInstanceId int64
	JobName       string
	JobContext    *string
	CreateTime    time.Time
}

type batchStepExecution struct {
	StepExecutionId  int64
	JobExecutionId   int64
	JobInstanceId    int64
	JobName          string
	StepName         string
	CreateTime       time.Time
	StartTime        time.Time
	EndTime          time.Time
	Status           string
	ReadCount        int64
	WriteCount       int64
	CommitCount      int64
	FilterCount      int64
	ReadSkipCount    int64
	WriteSkipCount   int64
	ProcessSkipCount int64
	RollbackCount    int64
	ExecutionContext string
	ExitCode         string
	ExitMessage      string
	LastUpdated      time.Time
	Version          int64
}

type batchStepContext struct {
	ContextId     int64
	JobInstanceId int64
	StepName      string
	StepContext   *string
	CreateTime    time.Time
}

//load or save job instance by name & parameters
func findJobInstance(jobName string, params map[string]interface{}) (*batchJobInstance, BatchError) {
	str, err := util.JsonString(params)
	if err != nil {
		return nil, NewBatchError(ErrCodeGeneral, err)
	}
	key := util.MD5(str)
	rows, err := db.Query("select job_instance_id, job_name, job_key, job_params, create_time from batch_job_instance where job_name=? and job_key=?", jobName, key)
	if err != nil {
		return nil, NewBatchError(ErrCodeDbFail, err)
	}
	defer rows.Close()

	for rows.Next() {
		inst := &batchJobInstance{}
		err = rows.Scan(&inst.JobInstanceId, &inst.JobName, &inst.JobKey, &inst.JobParams, &inst.CreateTime)
		if err != nil {
			return nil, NewBatchError(ErrCodeDbFail, err)
		}
		return inst, nil
	}
	return nil, nil
}

func findLastJobInstanceByName(jobName string) (*batchJobInstance, BatchError) {
	rows, err := db.Query("select job_instance_id, job_name, job_key, job_params, create_time from batch_job_instance where job_name=? order by job_instance_id desc", jobName)
	if err != nil {
		return nil, NewBatchError(ErrCodeDbFail, err)
	}
	defer rows.Close()

	if rows.Next() {
		inst := &batchJobInstance{}
		err = rows.Scan(&inst.JobInstanceId, &inst.JobName, &inst.JobKey, &inst.JobParams, &inst.CreateTime)
		if err != nil {
			return nil, NewBatchError(ErrCodeDbFail, err)
		}
		return inst, nil
	}
	return nil, nil
}

func createJobInstance(jobName string, jobParams map[string]interface{}) (*batchJobInstance, BatchError) {
	str, err := util.JsonString(jobParams)
	if err != nil {
		return nil, NewBatchError(ErrCodeGeneral, err)
	}
	key := util.MD5(str)
	jobInstance := &batchJobInstance{
		JobName:    jobName,
		JobKey:     key,
		JobParams:  str,
		CreateTime: time.Now(),
	}
	res, err := db.Exec("insert into batch_job_instance(job_name, job_key, job_params, create_time) values(?, ?, ?, ?)", jobName, key, str, jobInstance.CreateTime)
	if err != nil {
		return nil, NewBatchError(ErrCodeDbFail, err)
	}
	id, er := res.LastInsertId()
	if er == nil {
		jobInstance.JobInstanceId = id
	}
	return jobInstance, nil
}

//load or save job executions by instance
func findLastJobExecutionByInstance(jobInstance *batchJobInstance) (*JobExecution, BatchError) {
	rows, err := db.Query("select job_execution_id, job_instance_id, job_name, create_time, start_time, end_time, status, exit_code, exit_message, last_updated, version from batch_job_execution where job_instance_id=? order by job_execution_id desc", jobInstance.JobInstanceId)
	if err != nil {
		return nil, NewBatchError(ErrCodeDbFail, err)
	}
	defer rows.Close()

	jobParams, _ := parseJobParams(jobInstance.JobParams)
	if rows.Next() {
		execution := &batchJobExecution{}
		err = rows.Scan(&execution.JobExecutionId, &execution.JobInstanceId, &execution.JobName, &execution.CreateTime, &execution.StartTime, &execution.EndTime, &execution.Status, &execution.ExitCode, &execution.ExitMessage, &execution.LastUpdated, &execution.Version)
		if err != nil {
			return nil, NewBatchError(ErrCodeDbFail, err)
		}
		jobExecution := &JobExecution{
			JobExecutionId: execution.JobExecutionId,
			JobInstanceId:  execution.JobExecutionId,
			JobName:        execution.JobName,
			JobParams:      jobParams,
			JobStatus:      status.BatchStatus(execution.Status),
			CreateTime:     execution.CreateTime,
			StartTime:      execution.StartTime,
			EndTime:        execution.EndTime,
			Version:        execution.Version,
		}
		return jobExecution, nil
	}
	return nil, nil
}

func findJobExecution(jobExecutionId int64) (*JobExecution, BatchError) {
	rows, err := db.Query("select job_execution_id, job_instance_id, job_name, create_time, start_time, end_time, status, exit_code, exit_message, last_updated, version from batch_job_execution where job_execution_id=?", jobExecutionId)
	if err != nil {
		return nil, NewBatchError(ErrCodeDbFail, err)
	}
	defer rows.Close()

	if rows.Next() {
		execution := &batchJobExecution{}
		err = rows.Scan(&execution.JobExecutionId, &execution.JobInstanceId, &execution.JobName, &execution.CreateTime, &execution.StartTime, &execution.EndTime, &execution.Status, &execution.ExitCode, &execution.ExitMessage, &execution.LastUpdated, &execution.Version)
		if err != nil {
			return nil, NewBatchError(ErrCodeDbFail, err)
		}
		jobExecution := &JobExecution{
			JobExecutionId: execution.JobExecutionId,
			JobInstanceId:  execution.JobExecutionId,
			JobName:        execution.JobName,
			JobStatus:      status.BatchStatus(execution.Status),
			CreateTime:     execution.CreateTime,
			StartTime:      execution.StartTime,
			EndTime:        execution.EndTime,
			Version:        execution.Version,
		}
		return jobExecution, nil
	}
	return nil, nil
}

func saveJobExecutions(executions ...*JobExecution) BatchError {
	buff := bytes.NewBufferString("")
	args := make([]interface{}, 0)
	for _, execution := range executions {
		if execution.JobExecutionId == 0 {
			buff.WriteString("insert into batch_job_execution(job_instance_id, job_name, create_time, status, exit_code, exit_message, last_updated, version) values")
			buff.WriteString("(?, ?, ?, ?, ?, ?, ?, ?)")
			args = append(args, execution.JobInstanceId, execution.JobName, execution.CreateTime, string(execution.JobStatus), execution.JobStatus, execution.FailError, time.Now(), 1)
			res, err := db.Exec(buff.String(), args...)
			if err != nil {
				return NewBatchError(ErrCodeDbFail, err)
			}
			id, _ := res.LastInsertId()
			if id > 0 {
				execution.JobExecutionId = id
			}
			execution.Version = 1
		} else {
			buff.WriteString("update batch_job_execution set status=?, start_time=?, end_time=?, exit_code=?, exit_message=?, last_updated=?, version=? where job_execution_id=? and version=?")
			args = append(args, execution.JobStatus, execution.StartTime, execution.EndTime, execution.JobStatus, execution.FailError, time.Now(), execution.Version+1, execution.JobExecutionId, execution.Version)
			res, err := db.Exec(buff.String(), args...)
			if err != nil {
				return NewBatchError(ErrCodeDbFail, err)
			}
			rowsAffected, _ := res.RowsAffected()
			if rowsAffected <= 0 {
				return NewBatchError(ErrCodeDbFail, errors.Errorf("update batch_job_execution failed:%v", execution))
			}
			execution.Version += 1
		}
		buff.Reset()
		args = args[0:0]
	}
	return nil
}

func checkJobStopping(execution *JobExecution) (bool, BatchError) {
	storeExecution, err := findJobExecution(execution.JobExecutionId)
	if err != nil || storeExecution == nil {
		return false, err
	}
	stoppping := storeExecution.Version != execution.Version || storeExecution.JobStatus == status.STOPPING
	if stoppping {
		execution.Version = storeExecution.Version
	}
	return stoppping, nil
}

func findStepExecutionsByJobExecution(jobExecutionId int64) ([]*StepExecution, BatchError) {
	rows, err := db.Query("select step_execution_id, step_name, job_execution_id, job_instance_id, job_name, create_time, start_time, end_time, status, commit_count, read_count, filter_count, write_count, read_skip_count, write_skip_count, process_skip_count, rollback_count, execution_context, exit_code, exit_message, last_updated, version from batch_step_execution where job_execution_id=? order by step_execution_id desc", jobExecutionId)
	if err != nil {
		return nil, NewBatchError(ErrCodeDbFail, err)
	}
	defer rows.Close()

	results := make([]*StepExecution, 0)
	for rows.Next() {
		//1. query step execution
		execution := &batchStepExecution{}
		err = rows.Scan(&execution.StepExecutionId, &execution.StepName, &execution.JobExecutionId, &execution.JobInstanceId, &execution.JobName, &execution.CreateTime, &execution.StartTime, &execution.EndTime, &execution.Status, &execution.CommitCount, &execution.ReadCount, &execution.FilterCount, &execution.WriteCount, &execution.ReadSkipCount, &execution.WriteSkipCount, &execution.ProcessSkipCount, &execution.RollbackCount, &execution.ExecutionContext, &execution.ExitCode, &execution.ExitMessage, &execution.LastUpdated, &execution.Version)
		if err != nil {
			return nil, NewBatchError(ErrCodeDbFail, err)
		}
		//2. query step context
		batchStepCtx, err := findStepContext(execution.JobInstanceId, execution.StepName)
		if err != nil {
			return nil, err
		}
		if batchStepCtx == nil {
			return nil, NewBatchError(ErrCodeGeneral, errors.Errorf("can not find step execution for step:%v", execution.StepName))
		}
		//3. construct step execution
		stepContext := NewBatchContext()
		if er := util.ParseJson(*batchStepCtx.StepContext, stepContext); er != nil {
			return nil, NewBatchError(ErrCodeGeneral, er)
		}
		stepExecutionContext := NewBatchContext()
		if er := util.ParseJson(execution.ExecutionContext, stepExecutionContext); er != nil {
			return nil, NewBatchError(ErrCodeGeneral, er)
		}
		stepExecution := &StepExecution{
			StepExecutionId:      execution.StepExecutionId,
			StepName:             execution.StepName,
			StepStatus:           status.BatchStatus(execution.Status),
			StepContext:          stepContext,
			StepExecutionContext: stepExecutionContext,
			CreateTime:           execution.CreateTime,
			StartTime:            execution.StartTime,
			EndTime:              execution.EndTime,
			ReadCount:            execution.ReadCount,
			WriteCount:           execution.WriteCount,
			CommitCount:          execution.CommitCount,
			FilterCount:          execution.FilterCount,
			ReadSkipCount:        execution.ReadSkipCount,
			WriteSkipCount:       execution.WriteSkipCount,
			ProcessSkipCount:     execution.ProcessSkipCount,
			RollbackCount:        execution.RollbackCount,
			LastUpdated:          execution.LastUpdated,
			Version:              execution.Version,
		}
		results = append(results, stepExecution)
	}
	return results, nil
}

//load or save step executions by job_execution and step name
func findStepExecutionsByName(jobExecutionId int64, stepName string) (*StepExecution, BatchError) {
	rows, err := db.Query("select step_execution_id, step_name, job_execution_id, job_instance_id, job_name, create_time, start_time, end_time, status, commit_count, read_count, filter_count, write_count, read_skip_count, write_skip_count, process_skip_count, rollback_count, execution_context, exit_code, exit_message, last_updated, version from batch_step_execution where job_execution_id=? and step_name=? order by step_execution_id desc", jobExecutionId, stepName)
	if err != nil {
		return nil, NewBatchError(ErrCodeDbFail, err)
	}
	defer rows.Close()

	if rows.Next() {
		//1. query step execution
		execution := &batchStepExecution{}
		err = rows.Scan(&execution.StepExecutionId, &execution.StepName, &execution.JobExecutionId, &execution.JobInstanceId, &execution.JobName, &execution.CreateTime, &execution.StartTime, &execution.EndTime, &execution.Status, &execution.CommitCount, &execution.ReadCount, &execution.FilterCount, &execution.WriteCount, &execution.ReadSkipCount, &execution.WriteSkipCount, &execution.ProcessSkipCount, &execution.RollbackCount, &execution.ExecutionContext, &execution.ExitCode, &execution.ExitMessage, &execution.LastUpdated, &execution.Version)
		if err != nil {
			return nil, NewBatchError(ErrCodeDbFail, err)
		}
		//2. query step context
		batchStepCtx, err := findStepContext(execution.JobInstanceId, execution.StepName)
		if err != nil {
			return nil, err
		}
		if batchStepCtx == nil {
			return nil, NewBatchError(ErrCodeGeneral, errors.Errorf("can not find step execution for step:%v", stepName))
		}
		//3. construct step execution
		stepContext := NewBatchContext()
		if er := util.ParseJson(*batchStepCtx.StepContext, stepContext); er != nil {
			return nil, NewBatchError(ErrCodeGeneral, er)
		}
		stepExecutionContext := NewBatchContext()
		if er := util.ParseJson(execution.ExecutionContext, stepExecutionContext); er != nil {
			return nil, NewBatchError(ErrCodeGeneral, er)
		}
		stepExecution := &StepExecution{
			StepExecutionId:      execution.StepExecutionId,
			StepName:             execution.StepName,
			StepStatus:           status.BatchStatus(execution.Status),
			StepContext:          stepContext,
			StepExecutionContext: stepExecutionContext,
			CreateTime:           execution.CreateTime,
			StartTime:            execution.StartTime,
			EndTime:              execution.EndTime,
			ReadCount:            execution.ReadCount,
			WriteCount:           execution.WriteCount,
			CommitCount:          execution.CommitCount,
			FilterCount:          execution.FilterCount,
			ReadSkipCount:        execution.ReadSkipCount,
			WriteSkipCount:       execution.WriteSkipCount,
			ProcessSkipCount:     execution.ProcessSkipCount,
			RollbackCount:        execution.RollbackCount,
			LastUpdated:          execution.LastUpdated,
			Version:              execution.Version,
		}
		return stepExecution, nil
	}
	return nil, nil
}

func saveStepExecution(ctx context.Context, execution *StepExecution) BatchError {
	buff := bytes.NewBufferString("")
	args := make([]interface{}, 0)
	//1. save step context
	if !execution.StepExecutionContext.Exists("step_context_id") {
		stepCtxJson, _ := util.JsonString(execution.StepContext)
		stepContext := &batchStepContext{
			JobInstanceId: execution.JobExecution.JobExecutionId,
			StepName:      execution.StepName,
			StepContext:   &stepCtxJson,
			CreateTime:    time.Now(),
		}
		if err := saveStepContexts(stepContext); err != nil {
			return err
		}
		execution.StepExecutionContext.Put("step_context_id", stepContext.ContextId)
	}
	//2. save step execution
	if execution.StepExecutionId == 0 {
		buff.WriteString("insert into batch_step_execution(step_name, job_execution_id, job_instance_id, job_name, create_time, status, commit_count, read_count, filter_count, write_count, read_skip_count, write_skip_count, process_skip_count, rollback_count, execution_context, exit_code, exit_message, last_updated, version) values")
		buff.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		executionCtxJson, err := util.JsonString(execution.StepExecutionContext)
		if err != nil {
			return NewBatchError(ErrCodeGeneral, err)
		}
		args = append(args, execution.StepName, execution.JobExecution.JobExecutionId, execution.JobExecution.JobInstanceId, execution.JobExecution.JobName, execution.CreateTime, execution.StepStatus, execution.CommitCount, execution.ReadCount, execution.FilterCount, execution.WriteCount, execution.ReadSkipCount, execution.WriteSkipCount, execution.ProcessSkipCount, execution.RollbackCount, executionCtxJson, execution.StepStatus, execution.FailError, time.Now(), 1)

		res, err := db.Exec(buff.String(), args...)
		if err != nil {
			return NewBatchError(ErrCodeDbFail, err)
		}
		//3. fill back step execution
		id, _ := res.LastInsertId()
		if id > 0 {
			execution.StepExecutionId = id
		}
		execution.Version = 1
	} else {
		executionCtxStr, _ := util.JsonString(execution.StepExecutionContext)
		buff.WriteString("update batch_step_execution set status=?, commit_count=?, read_count=?, filter_count=?, write_count=?, read_skip_count=?, write_skip_count=?, process_skip_count=?, rollback_count=?, execution_context=?, last_updated=?, version=? where step_execution_id=? and version=?")
		args = append(args, execution.StepStatus, execution.CommitCount, execution.ReadCount, execution.FilterCount, execution.WriteCount, execution.ReadSkipCount, execution.WriteSkipCount, execution.ProcessSkipCount, execution.RollbackCount, executionCtxStr, time.Now(), execution.Version+1, execution.StepExecutionId, execution.Version)

		res, err := db.Exec(buff.String(), args...)
		if err != nil {
			return NewBatchError(ErrCodeDbFail, err)
		}
		rowsAffected, _ := res.RowsAffected()
		if rowsAffected <= 0 {
			return  NewBatchError(ErrCodeConcurrency, errors.Errorf("update batch_step_execution failed:%v", execution))
		}
		execution.Version += 1
	}
	//3. check job status
	stopping, err := checkJobStopping(execution.JobExecution)
	if err != nil {
		return err
	}
	if stopping && execution.StepStatus != status.STOPPED {
		oldStatus := execution.StepStatus
		execution.StepStatus = status.STOPPED
		execution.EndTime = time.Now()
		err = updateStepStatus(execution)
		i := 0
		for err != nil && i < 3 {
			err = updateStepStatus(execution)
			logger.Warn(ctx, "update step status to STOPPED failed for %d times, step:%v, err:%v", i, execution.StepName, err)
		}
		if err != nil {
			execution.StepStatus = oldStatus
			logger.Error(ctx, "update step status to STOPPED failed, step:%v, err:%v", execution.StepName, err)
		}
		return StopError
	}
	return nil
}

func updateStepStatus(execution *StepExecution) BatchError {
	buff := bytes.NewBufferString("")
	args := make([]interface{}, 0)
	buff.WriteString("update batch_step_execution set status=?, version=? where step_execution_id=? and version=?")
	args = append(args, execution.StepStatus, execution.Version+1, execution.StepExecutionId, execution.Version)

	res, err := db.Exec(buff.String(), args...)
	if err != nil {
		return NewBatchError(ErrCodeDbFail, err)
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected <= 0 {
		return NewBatchError(ErrCodeDbFail, errors.Errorf("update batch_step_execution status failed:%v", execution))
	}
	execution.Version += 1
	return nil
}

//load or save step executions by instance

//load or save step context
func findStepContext(jobInstanceId int64, stepName string) (*batchStepContext, BatchError) {
	rows, err := db.Query("select context_id, job_instance_id, step_name, step_context, create_time from batch_step_context where job_instance_id=? and step_name=?", jobInstanceId, stepName)
	if err != nil {
		return nil, NewBatchError(ErrCodeDbFail, err)
	}
	defer rows.Close()

	if rows.Next() {
		stepCtx := &batchStepContext{}
		err = rows.Scan(&stepCtx.ContextId, &stepCtx.JobInstanceId, &stepCtx.StepName, &stepCtx.StepContext, &stepCtx.CreateTime)
		if err != nil {
			return nil, NewBatchError(ErrCodeDbFail, err)
		}
		return stepCtx, nil
	}
	return nil, nil
}

func saveStepContexts(stepCtxs ...*batchStepContext) BatchError {
	buff := bytes.NewBufferString("")
	args := make([]interface{}, 0)
	for _, stepCtx := range stepCtxs {
		buff.WriteString("insert into batch_step_context(job_instance_id, step_name, step_context, create_time) values")
		buff.WriteString("(?, ?, ?, ?)")
		args = append(args, stepCtx.JobInstanceId, stepCtx.StepName, stepCtx.StepContext, stepCtx.CreateTime)
		res, err := db.Exec(buff.String(), args...)
		if err != nil {
			return NewBatchError(ErrCodeDbFail, err)
		}
		id, er := res.LastInsertId()
		if er == nil {
			stepCtx.ContextId = id
		}
		buff.Reset()
		args = args[0:0]
	}
	return nil
}
