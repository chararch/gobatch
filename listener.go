package gobatch

type JobListener interface {
	BeforeJob(execution *JobExecution) BatchError
	AfterJob(execution *JobExecution) BatchError
}

type StepListener interface {
	BeforeStep(execution *StepExecution) BatchError
	AfterStep(execution *StepExecution) BatchError
}

type ChunkListener interface {
	BeforeChunk(context *ChunkContext) BatchError
	AfterChunk(context *ChunkContext) BatchError
	OnError(context *ChunkContext, err BatchError)
}

type PartitionListener interface {
	BeforePartition(execution *StepExecution) BatchError
	AfterPartition(execution *StepExecution, subExecutions []*StepExecution) BatchError
	OnError(execution *StepExecution, err BatchError)
}