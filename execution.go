package gobatch

import (
	"github.com/chararch/gobatch/status"
	"time"
)

//JobExecution represents context of a job execution
type JobExecution struct {
	JobExecutionId int64
	JobInstanceId  int64
	JobName        string
	JobParams      map[string]interface{}
	JobStatus      status.BatchStatus
	StepExecutions []*StepExecution
	JobContext     *BatchContext
	CreateTime     time.Time
	StartTime      time.Time
	EndTime        time.Time
	FailError      BatchError
	Version        int64
}

//AddStepExecution add a step execution in this job
func (e *JobExecution) AddStepExecution(execution *StepExecution) {
	e.StepExecutions = append(e.StepExecutions, execution)
}

//StepExecution represents context of a step execution
type StepExecution struct {
	StepExecutionId      int64
	StepName             string
	StepStatus           status.BatchStatus
	StepContext          *BatchContext
	StepContextId        int64
	StepExecutionContext *BatchContext
	JobExecution         *JobExecution
	CreateTime           time.Time
	StartTime            time.Time
	EndTime              time.Time
	ReadCount            int64
	WriteCount           int64
	CommitCount          int64
	FilterCount          int64
	ReadSkipCount        int64
	WriteSkipCount       int64
	ProcessSkipCount     int64
	RollbackCount        int64
	FailError            BatchError
	LastUpdated          time.Time
	Version              int64
}

func (execution *StepExecution) finish(err BatchError) {
	if err != nil {
		execution.StepStatus = status.FAILED
		execution.FailError = err
		execution.EndTime = time.Now()
	} else {
		execution.StepStatus = status.COMPLETED
		execution.EndTime = time.Now()
	}
}

func (execution *StepExecution) start() {
	execution.StartTime = time.Now()
	execution.StepStatus = status.STARTED
}

func (execution *StepExecution) deepCopy() *StepExecution {
	result := &StepExecution{
		StepName:             execution.StepName,
		StepStatus:           status.STARTING,
		StepContext:          execution.StepContext.DeepCopy(),
		StepContextId:        execution.StepContextId,
		StepExecutionContext: execution.StepExecutionContext.DeepCopy(),
		JobExecution:         execution.JobExecution,
		CreateTime:           time.Now(),
	}
	return result
}
