package gobatch

// JobListener defines callbacks for job lifecycle events
type JobListener interface {
	// BeforeJob is called before a job execution starts
	BeforeJob(execution *JobExecution) BatchError
	// AfterJob is called after a job execution completes, regardless of success or failure
	AfterJob(execution *JobExecution) BatchError
}

// StepListener defines callbacks for step lifecycle events
type StepListener interface {
	// BeforeStep is called before a step execution starts
	BeforeStep(execution *StepExecution) BatchError
	// AfterStep is called after a step execution completes, regardless of success or failure
	AfterStep(execution *StepExecution) BatchError
}

// ChunkListener defines callbacks for chunk processing events in chunk-oriented steps
type ChunkListener interface {
	// BeforeChunk is called before processing a chunk in a chunk-oriented step
	BeforeChunk(context *ChunkContext) BatchError
	// AfterChunk is called after processing a chunk in a chunk-oriented step
	AfterChunk(context *ChunkContext) BatchError
	// OnError is called when an error occurs during chunk processing
	OnError(context *ChunkContext, err BatchError)
}

// PartitionListener defines callbacks for partition processing events
type PartitionListener interface {
	// BeforePartition is called before the Partitioner.Partition() method is invoked
	BeforePartition(execution *StepExecution) BatchError
	// AfterPartition is called after the Partitioner.Partition() method returns successfully
	AfterPartition(execution *StepExecution, subExecutions []*StepExecution) BatchError
	// OnError is called when Partitioner.Partition() returns an error
	OnError(execution *StepExecution, err BatchError)
}
