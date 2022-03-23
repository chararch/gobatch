package gobatch

import (
	"fmt"
	"github.com/chararch/gobatch/file"
)

const (
	//DefaultChunkSize default number of record per chunk to read
	DefaultChunkSize = 10
	//DefaultPartitions default number of partitions to construct a step
	DefaultPartitions = 1

	//DefaultMinPartitionSize default min number of record to process in a sub step of a partitionStep
	DefaultMinPartitionSize = 1
	//DefaultMaxPartitionSize default max number of record to process in a sub step of a partitionStep
	DefaultMaxPartitionSize = 2147483647
)

type stepBuilder struct {
	name               string
	task               Task
	handler            Handler
	reader             Reader
	processor          Processor
	writer             Writer
	chunkSize          uint
	partitioner        Partitioner
	partitions         uint
	minPartitionSize   uint
	maxPartitionSize   uint
	aggregator         Aggregator
	stepListeners      []StepListener
	chunkListeners     []ChunkListener
	partitionListeners []PartitionListener
}

//NewStep initialize a step builder
func NewStep(name string, handler ...interface{}) *stepBuilder {
	if name == "" {
		panic("step name must not be empty")
	}
	builder := &stepBuilder{
		name:               name,
		processor:          &nilProcessor{},
		writer:             &nilWriter{},
		chunkSize:          DefaultChunkSize,
		partitions:         DefaultPartitions,
		minPartitionSize:   DefaultMinPartitionSize,
		maxPartitionSize:   DefaultMaxPartitionSize,
		stepListeners:      make([]StepListener, 0),
		chunkListeners:     make([]ChunkListener, 0),
		partitionListeners: make([]PartitionListener, 0),
	}
	if len(handler) > 0 {
		for _, h := range handler {
			builder.Handler(h)
		}
	}

	return builder
}

func (builder *stepBuilder) Handler(handler interface{}) *stepBuilder {
	valid := false
	switch val := handler.(type) {
	case Task:
		builder.Task(val)
		valid = true
	case func(execution *StepExecution) BatchError:
		builder.Task(val)
		valid = true
	case func(execution *StepExecution):
		builder.Task(func(execution *StepExecution) BatchError {
			val(execution)
			return nil
		})
		valid = true
	case func() error:
		builder.Task(func(execution *StepExecution) BatchError {
			if e := val(); e != nil {
				switch et := e.(type) {
				case BatchError:
					return et
				default:
					return NewBatchError(ErrCodeGeneral, "execute step:%v error", execution.StepName, e)
				}
			}
			return nil
		})
		valid = true
	case func():
		builder.Task(func(execution *StepExecution) BatchError {
			val()
			return nil
		})
		valid = true
	case Handler:
		builder.handler = val
		valid = true
	default:
		if val2, ok2 := handler.(Reader); ok2 {
			builder.Reader(val2)
			valid = true
		}
		if val2, ok2 := handler.(ItemReader); ok2 {
			builder.Reader(val2)
			valid = true
		}
		if val2, ok2 := handler.(Processor); ok2 {
			builder.Processor(val2)
			valid = true
		}
		if val2, ok2 := handler.(Writer); ok2 {
			builder.Writer(val2)
			valid = true
		}
		if val2, ok2 := handler.(Partitioner); ok2 {
			builder.Partitioner(val2)
			valid = true
		}
		if val2, ok2 := handler.(Aggregator); ok2 {
			builder.Aggregator(val2)
			valid = true
		}
		if val2, ok2 := handler.(StepListener); ok2 {
			builder.stepListeners = append(builder.stepListeners, val2)
			valid = true
		}
		if val2, ok2 := handler.(ChunkListener); ok2 {
			builder.chunkListeners = append(builder.chunkListeners, val2)
			valid = true
		}
		if val2, ok2 := handler.(PartitionListener); ok2 {
			builder.partitionListeners = append(builder.partitionListeners, val2)
			valid = true
		}
	}
	if !valid {
		panic("invalid handler type")
	}

	return builder
}

func (builder *stepBuilder) Task(task Task) *stepBuilder {
	builder.task = task
	return builder
}

func (builder *stepBuilder) Reader(reader interface{}) *stepBuilder {
	switch r := reader.(type) {
	case Reader:
		builder.reader = r
	case ItemReader:
		builder.reader = &defaultChunkReader{
			itemReader: r,
		}
	default:
		panic("the type of Reader() argument is neither Reader nor ItemReader")
	}
	return builder
}

func (builder *stepBuilder) Processor(processor Processor) *stepBuilder {
	builder.processor = processor
	return builder
}

func (builder *stepBuilder) Writer(writer Writer) *stepBuilder {
	builder.writer = writer
	return builder
}

func (builder *stepBuilder) ReadFile(fd file.FileObjectModel, readers ...interface{}) *stepBuilder {
	fr := &fileReader{fd: fd}
	if len(readers) > 0 {
		for _, r := range readers {
			switch rr := r.(type) {
			case file.FileItemReader:
				fr.reader = rr
			case file.ChecksumVerifier:
				fr.verifier = rr
			}
		}
	}
	if fr.reader == nil && fr.fd.Type != "" {
		fr.reader = file.GetFileItemReader(fr.fd.Type)
	}
	if fr.reader == nil {
		panic("file type is non-standard and no FileItemReader specified")
	}
	builder.reader = fr
	return builder
}

func (builder *stepBuilder) WriteFile(fd file.FileObjectModel, writers ...interface{}) *stepBuilder {
	fw := &fileWriter{fd: fd}
	if len(writers) > 0 {
		for _, w := range writers {
			switch ww := w.(type) {
			case file.FileItemWriter:
				fw.writer = ww
			case file.ChecksumFlusher:
				fw.checkumer = ww
			case file.FileMerger:
				fw.merger = ww
			}
		}
	}
	if fw.writer == nil && fw.fd.Type != "" {
		fw.writer = file.GetFileItemWriter(fw.fd.Type)
	}
	if fw.writer == nil {
		panic("file type is non-standard and no FileItemWriter specified")
	}
	if fw.merger == nil && fw.fd.Type != "" {
		fw.merger = file.GetFileMergeSplitter(fw.fd.Type)
	}
	builder.writer = fw
	return builder
}

func (builder *stepBuilder) CopyFile(filesToMove ...file.FileMove) *stepBuilder {
	builder.handler = &fileCopyHandler{filesToMove: filesToMove}
	return builder
}

func (builder *stepBuilder) ChunkSize(chunkSize uint) *stepBuilder {
	builder.chunkSize = chunkSize
	return builder
}

func (builder *stepBuilder) Partitioner(partitioner Partitioner) *stepBuilder {
	builder.partitioner = partitioner
	return builder
}

func (builder *stepBuilder) Partitions(partitions uint, partitionSize ...uint) *stepBuilder {
	builder.partitions = partitions
	if len(partitionSize) == 1 {
		builder.minPartitionSize = partitionSize[0]
		builder.maxPartitionSize = partitionSize[0]
	}
	if len(partitionSize) > 1 {
		builder.minPartitionSize = partitionSize[0]
		builder.maxPartitionSize = partitionSize[1]
	}
	return builder
}

func (builder *stepBuilder) Aggregator(aggregator Aggregator) *stepBuilder {
	builder.aggregator = aggregator
	return builder
}

func (builder *stepBuilder) Listener(listener ...interface{}) *stepBuilder {
	for _, l := range listener {
		switch ll := l.(type) {
		case StepListener:
			builder.stepListeners = append(builder.stepListeners, ll)
		case ChunkListener:
			builder.chunkListeners = append(builder.chunkListeners, ll)
		case PartitionListener:
			builder.partitionListeners = append(builder.partitionListeners, ll)
		default:
			panic(fmt.Sprintf("not supported listener:%+v for step:%v", ll, builder.name))
		}
	}
	return builder
}

func (builder *stepBuilder) Build() Step {
	var step Step
	if builder.handler != nil {
		step = newSimpleStep(builder.name, builder.handler, builder.stepListeners)
	} else if builder.task != nil {
		step = newSimpleStep(builder.name, builder.task, builder.stepListeners)
	} else if builder.reader != nil {
		if txManager == nil {
			panic(fmt.Sprintf("you must specify a transaction manager with gobatch.SetTransactionManager() before constructing chunk step:%v", builder.name))
		}
		reader := builder.reader
		writer := builder.writer
		step = newChunkStep(builder.name, reader, builder.processor, writer, builder.chunkSize, builder.stepListeners, builder.chunkListeners)
	}

	if step != nil {
		if builder.partitioner != nil {
			step = newPartitionStep(step, builder.partitioner, builder.partitions, builder.aggregator, builder.stepListeners, builder.partitionListeners)
		} else if builder.partitions > 1 {
			if builder.reader != nil {
				if r, ok := builder.reader.(PartitionerFactory); ok {
					partitioner := r.GetPartitioner(builder.minPartitionSize, builder.maxPartitionSize)
					aggregator := builder.aggregator
					if aggregator == nil && builder.writer != nil {
						if aggr, ok2 := builder.writer.(Aggregator); ok2 {
							aggregator = aggr
						}
					}
					step = newPartitionStep(step, partitioner, builder.partitions, aggregator, builder.stepListeners, builder.partitionListeners)
				} else {
					panic(fmt.Sprintf("can not partition step[%s] without Partitioner or PartitionerFactory\n", builder.name))
				}
			} else {
				panic(fmt.Sprintf("can not partition step[%s] without Partitioner or PartitionerFactory\n", builder.name))
			}
		}
	}
	if step == nil {
		panic(fmt.Sprintf("no handler or reader specified for step: %s\n", builder.name))
	}

	return step
}

type nilProcessor struct {
}

func (p *nilProcessor) Process(item interface{}, chunkCtx *ChunkContext) (interface{}, BatchError) {
	return item, nil
}

type nilWriter struct {
}

func (w *nilWriter) Open(execution *StepExecution) BatchError {
	return nil
}
func (w *nilWriter) Write(items []interface{}, chunkCtx *ChunkContext) BatchError {
	return nil
}
func (w *nilWriter) Close(execution *StepExecution) BatchError {
	return nil
}
