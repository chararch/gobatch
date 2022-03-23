package gobatch

import (
	"context"
	"github.com/chararch/gobatch/status"
	"reflect"
	"runtime/debug"
	"time"
)

//Job job interface used by GoBatch
type Job interface {
	Name() string
	Start(ctx context.Context, execution *JobExecution) BatchError
	Stop(ctx context.Context, execution *JobExecution) BatchError
	GetSteps() []Step
}

type simpleJob struct {
	name      string
	steps     []Step
	listeners []JobListener
}

func newSimpleJob(name string, steps []Step, listeners []JobListener) *simpleJob {
	return &simpleJob{
		name:      name,
		steps:     steps,
		listeners: listeners,
	}
}

func (job *simpleJob) Name() string {
	return job.name
}

func (job *simpleJob) Start(ctx context.Context, execution *JobExecution) (err BatchError) {
	defer func() {
		if er := recover(); er != nil {
			logger.Error(ctx, "panic in job executing, jobName:%v, jobExecutionId:%v, err:%v, stack:%v", job.name, execution.JobExecutionId, er, string(debug.Stack()))
			execution.JobStatus = status.FAILED
			execution.FailError = NewBatchError(ErrCodeGeneral, "panic in job execution", er)
			execution.EndTime = time.Now()
		}
		if err != nil {
			execution.JobStatus = status.FAILED
			execution.FailError = err
			execution.EndTime = time.Now()
		}
		if err = saveJobExecution(execution); err != nil {
			logger.Error(ctx, "save job execution failed, jobName:%v, JobExecution:%+v, err:%v", job.name, execution, err)
		}
	}()
	logger.Info(ctx, "start running job, jobName:%v, jobExecutionId:%v", job.name, execution.JobExecutionId)
	for _, listener := range job.listeners {
		err = listener.BeforeJob(execution)
		if err != nil {
			logger.Error(ctx, "job listener execute err, jobName:%v, jobExecutionId:%+v, listener:%v, err:%v", job.name, execution.JobExecutionId, reflect.TypeOf(listener).String(), err)
			execution.JobStatus = status.FAILED
			execution.FailError = err
			execution.EndTime = time.Now()
			return nil
		}
	}
	execution.JobStatus = status.STARTED
	execution.StartTime = time.Now()
	if err = saveJobExecution(execution); err != nil {
		logger.Error(ctx, "save job execution failed, jobName:%v, JobExecution:%+v, err:%v", job.name, execution, err)
		return err
	}
	jobStatus := status.COMPLETED
	for _, step := range job.steps {
		e := execStep(ctx, step, execution)
		if e != nil {
			logger.Error(ctx, "execute step failed, jobExecutionId:%v, step:%v, err:%v", execution.JobExecutionId, step.Name(), err)
			if e.Code() == ErrCodeStop {
				jobStatus = status.STOPPED
			} else {
				jobStatus = status.FAILED
			}
			break
		}
		if execution.JobStatus == status.FAILED || execution.JobStatus == status.UNKNOWN {
			jobStatus = execution.JobStatus
			break
		}
	}
	execution.JobStatus = jobStatus
	execution.EndTime = time.Now()
	for _, listener := range job.listeners {
		err = listener.AfterJob(execution)
		if err != nil {
			logger.Error(ctx, "job listener execute err, jobName:%v, jobExecutionId:%+v, listener:%v, err:%v", job.name, execution.JobExecutionId, reflect.TypeOf(listener).String(), err)
			execution.JobStatus = status.FAILED
			execution.FailError = err
			execution.EndTime = time.Now()
			break
		}
	}
	logger.Info(ctx, "finish job execution, jobName:%v, jobExecutionId:%v, jobStatus:%v", job.name, execution.JobExecutionId, execution.JobStatus)
	return nil
}

func execStep(ctx context.Context, step Step, execution *JobExecution) (err BatchError) {
	defer func() {
		if err != nil && err.Code() == ErrCodeStop {
			logger.Error(ctx, "error in step executing, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecutionId, step.Name(), err)
		}
	}()
	lastStepExecution, er := findLastStepExecution(execution.JobInstanceId, step.Name())
	if er != nil {
		err = er
		logger.Error(ctx, "find last StepExecution failed, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecutionId, step.Name(), er)
		return er
	}
	if lastStepExecution != nil && lastStepExecution.StepStatus == status.COMPLETED {
		logger.Info(ctx, "skip completed step, jobExecutionId:%v, stepName:%v", execution.JobExecutionId, step.Name())
		return nil
	}
	if lastStepExecution != nil && (lastStepExecution.StepStatus == status.STARTING || lastStepExecution.StepStatus == status.STARTED || lastStepExecution.StepStatus == status.STOPPING) {
		logger.Error(ctx, "last StepExecution is in progress, jobExecutionId:%v, stepName:%v", execution.JobExecutionId, step.Name())
		return NewBatchError(ErrCodeConcurrency, "last StepExecution of the Step:%v is in progress", step.Name())
	}
	stepExecution := &StepExecution{
		StepName:             step.Name(),
		StepStatus:           status.STARTING,
		StepContext:          NewBatchContext(),
		StepExecutionContext: NewBatchContext(),
		JobExecution:         execution,
		CreateTime:           time.Now(),
	}
	if lastStepExecution != nil {
		stepExecution.StepContext.Merge(lastStepExecution.StepContext)
		stepExecution.StepContextId = lastStepExecution.StepContextId
		stepExecution.StepExecutionContext.Merge(lastStepExecution.StepExecutionContext)
	}
	e := saveStepExecution(ctx, stepExecution)
	if e != nil {
		logger.Error(ctx, "save step execution failed, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecutionId, step.Name(), e)
		err = e
		return err
	}
	execution.AddStepExecution(stepExecution)
	err = step.Exec(ctx, stepExecution)
	if err != nil || stepExecution.StepStatus != status.COMPLETED {
		logger.Error(ctx, "step executing failed, jobExecutionId:%v, stepName:%v, stepStatus:%v, err:%v", execution.JobExecutionId, step.Name(), stepExecution.StepStatus, e)
		execution.JobStatus = stepExecution.StepStatus
		if err != nil && stepExecution.StepStatus != status.FAILED {
			stepExecution.StepStatus = status.FAILED
			stepExecution.FailError = err
			stepExecution.EndTime = time.Now()
			execution.JobStatus = status.FAILED
		}
		execution.JobStatus = status.FAILED
		execution.FailError = err
		execution.EndTime = time.Now()
		e = saveStepExecution(ctx, stepExecution)
		if e != nil {
			logger.Error(ctx, "save step execution failed, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecutionId, step.Name(), e)
			err = e
			return err
		}
	} else {
		stepExecution.StepStatus = status.COMPLETED
		stepExecution.EndTime = time.Now()
		e = saveStepExecution(ctx, stepExecution)
		if e != nil {
			logger.Error(ctx, "save step execution failed, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecutionId, step.Name(), e)
			err = e
			return err
		}
	}
	return nil
}

func (job *simpleJob) Stop(ctx context.Context, execution *JobExecution) BatchError {
	logger.Info(ctx, "stop job, jobName:%v, jobExecutionId:%v, jobStatus:%v", job.name, execution.JobExecutionId, execution.JobStatus)
	execution.JobStatus = status.STOPPING
	execution.EndTime = time.Now()
	return saveJobExecution(execution)
}

func (job *simpleJob) GetSteps() []Step {
	return job.steps
}
