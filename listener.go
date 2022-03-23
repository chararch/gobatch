package gobatch

//JobListener job listener
type JobListener interface {
	//BeforeJob execute before job start
	BeforeJob(execution *JobExecution) BatchError
	//AfterJob execute after job end either normally or abnormally
	AfterJob(execution *JobExecution) BatchError
}

//StepListener job listener
type StepListener interface {
	//BeforeStep execute before step start
	BeforeStep(execution *StepExecution) BatchError
	//AfterStep execute after step end either normally or abnormally
	AfterStep(execution *StepExecution) BatchError
}

//ChunkListener job listener
type ChunkListener interface {
	//BeforeChunk execute before start of a chunk in a chunkStep
	BeforeChunk(context *ChunkContext) BatchError
	//AfterChunk execute after end of a chunk in a chunkStep
	AfterChunk(context *ChunkContext) BatchError
	//OnError execute when an error occured during a chunk in a chunkStep
	OnError(context *ChunkContext, err BatchError)
}

//PartitionListener job listener
type PartitionListener interface {
	//BeforePartition execute before enter into Partitioner.Partition() in a partitionStep
	BeforePartition(execution *StepExecution) BatchError
	//AfterPartition execute after return from Partitioner.Partition() in a partitionStep
	AfterPartition(execution *StepExecution, subExecutions []*StepExecution) BatchError
	//OnError execute when an error return from Partitioner.Partition() in a partitionStep
	OnError(execution *StepExecution, err BatchError)
}
