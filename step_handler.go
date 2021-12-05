package gobatch

type Task func(execution *StepExecution) BatchError

type Handler interface {
	Handle(execution *StepExecution) BatchError
}

type Reader interface {
	Read(chunkCtx *ChunkContext) (interface{}, BatchError)
}
type Processor interface {
	Process(item interface{}, chunkCtx *ChunkContext) (interface{}, BatchError)
}
type Writer interface {
	Write(items []interface{}, chunkCtx *ChunkContext) BatchError
}

type Partitioner interface {
	Partition(execution *StepExecution, partitions uint) (map[string]*StepExecution, BatchError)
	GetPartitionNames(execution *StepExecution, partitions uint) []string
}