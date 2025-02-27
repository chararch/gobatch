package gobatch

// Task represents a function type that performs work in a simple step
type Task func(execution *StepExecution) BatchError

// Handler defines an interface for executing work in a simple step
type Handler interface {
	// Handle executes the core logic of the handler
	Handle(execution *StepExecution) BatchError
}

// Reader defines an interface for loading data in chunks
type Reader interface {
	// Read returns a single data item on each call.
	// Returns nil when no more data is available.
	// If an error occurs, returns nil item and BatchError
	Read(chunkCtx *ChunkContext) (interface{}, BatchError)
}

// Processor defines an interface for processing data in chunks
type Processor interface {
	// Process transforms an input item and returns a processed result
	Process(item interface{}, chunkCtx *ChunkContext) (interface{}, BatchError)
}

// Writer defines an interface for writing processed data in chunks
type Writer interface {
	// Write persists a batch of processed items in a single chunk
	Write(items []interface{}, chunkCtx *ChunkContext) BatchError
}

// OpenCloser defines lifecycle management operations for Readers and Writers
type OpenCloser interface {
	// Open performs initialization tasks for Reader or Writer
	Open(execution *StepExecution) BatchError
	// Close performs cleanup tasks for Reader or Writer
	Close(execution *StepExecution) BatchError
}

// Partitioner divides a step execution into multiple sub-executions
type Partitioner interface {
	// Partition creates sub-step executions based on the given execution and partition count
	Partition(execution *StepExecution, partitions uint) ([]*StepExecution, BatchError)
	// GetPartitionNames generates names for sub-steps based on execution and partition count
	GetPartitionNames(execution *StepExecution, partitions uint) []string
}

// PartitionerFactory creates Partitioner instances, typically used in conjunction with Readers
type PartitionerFactory interface {
	GetPartitioner(minPartitionSize, maxPartitionSize uint) Partitioner
}

// Aggregator combines results from multiple sub-step executions in a chunk step
type Aggregator interface {
	// Aggregate consolidates results from all sub-step executions into a final result
	Aggregate(execution *StepExecution, subExecutions []*StepExecution) BatchError
}
