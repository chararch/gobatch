package gobatch

import (
	"context"
	"fmt"
	"github.com/chararch/gobatch/status"
	"github.com/chararch/gobatch/util"
	"reflect"
	"runtime/debug"
	"time"
)

// Step step interface
type Step interface {
	Name() string
	Exec(ctx context.Context, execution *StepExecution) BatchError
	addListener(listener StepListener)
}

// simpleStep simple step implementation for internal use
type simpleStep struct {
	name      string
	handler   Handler
	listeners []StepListener
}

type handlerAdapter struct {
	task Task
}

func (h *handlerAdapter) Handle(execution *StepExecution) BatchError {
	return h.task(execution)
}

func newSimpleStep(name string, handler interface{}, listeners []StepListener) *simpleStep {
	switch h := handler.(type) {
	case Handler:
		return &simpleStep{
			name:      name,
			handler:   h,
			listeners: listeners,
		}
	case Task:
		return &simpleStep{
			name: name,
			handler: &handlerAdapter{
				task: h,
			},
			listeners: listeners,
		}
	default:
		panic(fmt.Sprintf("not supported step handler:%v for:%v", handler, name))
	}

}

func (step *simpleStep) Name() string {
	return step.name
}

func (step *simpleStep) Exec(ctx context.Context, execution *StepExecution) (err BatchError) {
	defer func() {
		err = execEnd(ctx, execution, err, recover())
	}()
	logger.Info(ctx, "step execute start, jobExecutionId:%v, stepName:%v", execution.JobExecution.JobExecutionId, execution.StepName)
	for _, listener := range step.listeners {
		err = listener.BeforeStep(execution)
		if err != nil {
			logger.Error(ctx, "step listener executing error, jobExecutionId:%v, stepName:%v, listener:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, reflect.TypeOf(listener).String(), err)
			return err
		}
	}
	execution.start()
	e := saveStepExecution(ctx, execution)
	if e != nil {
		logger.Error(ctx, "save step execution failed, jobExecutionId:%v, stepName:%v, StepExecution:%+v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, execution, e)
		err = e
		return e
	}
	var be BatchError
	for {
		be = step.handler.Handle(execution)
		if be == nil || be.Code() != ErrCodeRetry {
			break
		}
		logger.Error(ctx, "step execute will continue, jobExecutionId:%v, stepName:%v", execution.JobExecution.JobExecutionId, execution.StepName)
	}
	if be != nil {
		logger.Error(ctx, "step execute failed, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, be)
	} else {
		logger.Info(ctx, "step execute completed, jobExecutionId:%v, stepName:%v", execution.JobExecution.JobExecutionId, execution.StepName)
	}
	execution.finish(be)
	for _, listener := range step.listeners {
		err = listener.AfterStep(execution)
		if err != nil {
			logger.Error(ctx, "step listener executing error, jobExecutionId:%v, stepName:%v, listener:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, reflect.TypeOf(listener).String(), err)
			execution.finish(err)
			break
		}
	}
	logger.Info(ctx, "step execute finish, jobExecutionId:%v, stepName:%v, stepStatus:%v", execution.JobExecution.JobExecutionId, execution.StepName, execution.StepStatus)
	return nil
}

func execEnd(ctx context.Context, execution *StepExecution, err BatchError, recoverErr interface{}) BatchError {
	if recoverErr != nil {
		logger.Error(ctx, "panic in step executing, jobExecutionId:%v, stepName:%v, err:%v, stack:%v", execution.JobExecution.JobExecutionId, execution.StepName, recoverErr, string(debug.Stack()))
		execution.StepStatus = status.FAILED
		execution.FailError = NewBatchError(ErrCodeGeneral, "panic in step execution", recoverErr)
		execution.EndTime = time.Now()
	}
	if err != nil && execution.StepStatus != status.FAILED && execution.StepStatus != status.UNKNOWN {
		logger.Error(ctx, "step executing error, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, err)
		execution.StepStatus = status.FAILED
		execution.FailError = err
		execution.EndTime = time.Now()
	}
	for i := 0; i < 3; i++ {
		e := saveStepExecution(ctx, execution)
		if e != nil && (e.Code() == ErrCodeDbFail || e.Code() == ErrCodeConcurrency) { //retry
			logger.Error(ctx, "save step execution failed and retry for recoverable err, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, e)
			continue
		}
		if e != nil {
			err = e
			logger.Error(ctx, "save step execution failed, jobExecutionId:%v, stepName:%v, StepExecution:%+v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, execution, e)
		}
		break
	}
	return err
}

func (step *simpleStep) addListener(listener StepListener) {
	step.listeners = append(step.listeners, listener)
}

// chunkStep step implementation that process data in chunk
type chunkStep struct {
	name           string
	reader         Reader
	processor      Processor
	writer         Writer
	chunkSize      uint
	listeners      []StepListener
	chunkListeners []ChunkListener
}

type chunk struct {
	items     []interface{}
	skipItems []interface{}
	end       bool
}

func newChunk() *chunk {
	return &chunk{
		items:     make([]interface{}, 0),
		skipItems: make([]interface{}, 0),
		end:       false,
	}
}

func (ch *chunk) skip(index int) {
	ch.skipItems = append(ch.skipItems, ch.items[index])
	size := len(ch.items)
	copy(ch.items[index:], ch.items[index+1:])
	ch.items = ch.items[:size-1]
}

func newChunkStep(name string, reader Reader, processor Processor, writer Writer, chunkSize uint, listeners []StepListener, chunkListeners []ChunkListener) *chunkStep {
	return &chunkStep{
		name:           name,
		reader:         reader,
		processor:      processor,
		writer:         writer,
		chunkSize:      chunkSize,
		listeners:      listeners,
		chunkListeners: chunkListeners,
	}
}

func (step *chunkStep) Name() string {
	return step.name
}

func (step *chunkStep) Exec(ctx context.Context, execution *StepExecution) (err BatchError) {
	defer func() {
		err = execEnd(ctx, execution, err, recover())
	}()
	logger.Info(ctx, "step execute start, jobExecutionId:%v, stepName:%v", execution.JobExecution.JobExecutionId, execution.StepName)
	for _, listener := range step.listeners {
		err = listener.BeforeStep(execution)
		if err != nil {
			logger.Error(ctx, "step listener executing error, jobExecutionId:%v, stepName:%v, listener:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, reflect.TypeOf(listener).String(), err)
			return err
		}
	}
	execution.start()
	e := saveStepExecution(ctx, execution)
	if e != nil {
		err = e
		logger.Error(ctx, "save step execution failed, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, e)
		return
	}
	err = step.doOpenIfNecessary(execution)
	if err != nil {
		logger.Error(ctx, "open resource failed, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, err)
		execution.finish(err)
		return err
	}
	defer func() {
		err = step.doCloseIfNecessary(execution)
		if err != nil {
			logger.Error(ctx, "close resource failed, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, err)
		}
	}()
	input := newChunk()
	output := newChunk()
	for !input.end {
		tx, txErr := txManager.BeginTx()
		if txErr != nil {
			logger.Error(ctx, "start transaction err, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, txErr)
			err = txErr
			break
		}
		chunkContext := &ChunkContext{
			StepExecution: execution,
			Tx:            tx,
		}
		err = step.doChunk(ctx, chunkContext, input, output)
		if err != nil {
			logger.Error(ctx, "doChunk err, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, err)
			txErr = txManager.Rollback(tx)
			if txErr != nil {
				logger.Error(ctx, "rollback transaction err, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, txErr)
			}
			break
		}
		txErr = txManager.Commit(tx)
		if txErr != nil {
			logger.Error(ctx, "commit transaction err, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, txErr)
			txErr2 := txManager.Rollback(tx)
			if txErr2 != nil {
				logger.Error(ctx, "rollback transaction err, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, txErr)
			}
			execution.RollbackCount++
			err = txErr
			e := saveStepExecution(ctx, execution)
			if e != nil {
				err = e
				logger.Error(ctx, "save step execution failed, jobExecutionId:%v, stepName:%v, execution:%+v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, execution, e)
			}
			break
		} else if len(input.items) > 0 {
			execution.ReadCount += int64(len(input.items))
			execution.WriteCount += int64(len(output.items))
			execution.FilterCount += int64(len(input.skipItems))
			execution.CommitCount++
			e := saveStepExecution(ctx, execution)
			if e != nil {
				err = e
				logger.Error(ctx, "save step execution failed, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, e)
				break
			}
		}
	}
	execution.finish(err)
	for _, listener := range step.listeners {
		err = listener.AfterStep(execution)
		if err != nil && execution.StepStatus != status.FAILED {
			logger.Error(ctx, "step listener executing error, jobExecutionId:%v, stepName:%v, listener:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, reflect.TypeOf(listener).String(), err)
			execution.finish(err)
			break
		}
	}
	logger.Info(ctx, "step execute finish, jobExecutionId:%v, stepName:%v, stepStatus:%v", execution.JobExecution.JobExecutionId, execution.StepName, execution.StepStatus)
	return nil
}

func (step *chunkStep) doOpenIfNecessary(execution *StepExecution) BatchError {
	if rc, ok := step.reader.(OpenCloser); ok {
		if err := rc.Open(execution); err != nil {
			return err
		}
	}
	if step.writer != nil {
		if wc, ok := step.writer.(OpenCloser); ok {
			if err := wc.Open(execution); err != nil {
				return err
			}
		}
	}
	return nil
}

func (step *chunkStep) doCloseIfNecessary(execution *StepExecution) BatchError {
	if rc, ok := step.reader.(OpenCloser); ok {
		if err := rc.Close(execution); err != nil {
			return err
		}
	}
	if step.writer != nil {
		if wc, ok := step.writer.(OpenCloser); ok {
			if err := wc.Close(execution); err != nil {
				return err
			}
		}
	}
	return nil
}

func (step *chunkStep) doChunk(ctx context.Context, chunk *ChunkContext, input *chunk, output *chunk) (err BatchError) {
	defer func() {
		if er := recover(); er != nil {
			logger.Error(ctx, "panic on chunk executing, jobExecutionId:%v, stepName:%v, err:%v, stack:%v", chunk.StepExecution.JobExecution.JobExecutionId, chunk.StepExecution.StepName, er, string(debug.Stack()))
			err = NewBatchError(ErrCodeGeneral, "panic on chunk executing, jobExecutionId:%v, stepName:%v", chunk.StepExecution.JobExecution.JobExecutionId, chunk.StepExecution.StepName, er)
		}
	}()
	logger.Debug(ctx, "doChunk start, jobExecutionId:%v, stepName:%v", chunk.StepExecution.JobExecution.JobExecutionId, chunk.StepExecution.StepName)
	for _, listener := range step.chunkListeners {
		err = listener.BeforeChunk(chunk)
		if err != nil {
			logger.Error(ctx, "chunk listener executing error, jobExecutionId:%v, stepName:%v, listener:%v, err:%v", chunk.StepExecution.JobExecution.JobExecutionId, chunk.StepExecution.StepName, reflect.TypeOf(listener).String(), err)
			return err
		}
	}
	err = readChunk(step.reader, chunk, input, step.chunkSize)
	if err != nil {
		logger.Error(ctx, "read chunk data error, jobExecutionId:%v, stepName:%v, err:%v", chunk.StepExecution.JobExecution.JobExecutionId, chunk.StepExecution.StepName, err)
		for _, listener := range step.chunkListeners {
			listener.OnError(chunk, err)
		}
		return err
	}
	logger.Debug(ctx, "read chunk data success, jobExecutionId:%v, stepName:%v, read count:%v", chunk.StepExecution.JobExecution.JobExecutionId, chunk.StepExecution.StepName, len(input.items))

	output.items = output.items[0:0]
	if len(input.items) > 0 {
		for index, item := range input.items {
			outItem := item
			if step.processor != nil {
				outItem, err = step.processor.Process(item, chunk)
				if err != nil {
					logger.Error(ctx, "process chunk item error, jobExecutionId:%v, stepName:%v, item:%v, err:%v", chunk.StepExecution.JobExecution.JobExecutionId, chunk.StepExecution.StepName, item, err)
					for _, listener := range step.chunkListeners {
						listener.OnError(chunk, err)
					}
					return err
				}
				logger.Debug(ctx, "process chunk item success, jobExecutionId:%v, stepName:%v, inItem:%v, outItem:%v", chunk.StepExecution.JobExecution.JobExecutionId, chunk.StepExecution.StepName, item, outItem)
			}
			if outItem != nil {
				output.items = append(output.items, outItem)
			} else {
				input.skip(index)
			}
		}
		if len(output.items) > 0 && step.writer != nil {
			err = step.writer.Write(output.items, chunk)
			if err != nil {
				logger.Error(ctx, "write chunk data error, jobExecutionId:%v, stepName:%v, err:%v", chunk.StepExecution.JobExecution.JobExecutionId, chunk.StepExecution.StepName, err)
				for _, listener := range step.chunkListeners {
					listener.OnError(chunk, err)
				}
				return err
			}
			logger.Debug(ctx, "write chunk data success, jobExecutionId:%v, stepName:%v, write count:%v", chunk.StepExecution.JobExecution.JobExecutionId, chunk.StepExecution.StepName, len(output.items))
		}
	}
	for _, listener := range step.chunkListeners {
		err = listener.AfterChunk(chunk)
		if err != nil {
			logger.Error(ctx, "chunk listener executing error, jobExecutionId:%v, stepName:%v, listener:%v, err:%v", chunk.StepExecution.JobExecution.JobExecutionId, chunk.StepExecution.StepName, reflect.TypeOf(listener).String(), err)
			return err
		}
	}
	return nil
}

func readChunk(reader Reader, chunk *ChunkContext, input *chunk, chunkSize uint) BatchError {
	input.end = false
	input.items = input.items[0:0]
	for i := 0; i < int(chunkSize); i++ {
		item, err := reader.Read(chunk)
		if err != nil {
			return err
		}
		if item != nil {
			input.items = append(input.items, item)
		} else {
			input.end = true
			break
		}
	}
	chunk.End = input.end
	return nil
}

func (step *chunkStep) addListener(listener StepListener) {
	step.listeners = append(step.listeners, listener)
}

func (step *chunkStep) addChunkListener(listener ChunkListener) {
	step.chunkListeners = append(step.chunkListeners, listener)
}

// partitionStep step implementation that can split process into multi sub processes which executed in parallel
type partitionStep struct {
	name               string
	step               Step
	partitions         uint
	partitioner        Partitioner
	aggregator         Aggregator
	listeners          []StepListener
	partitionListeners []PartitionListener
	taskPool           *taskPool
}

func newPartitionStep(step Step, partitioner Partitioner, partitions uint, aggregator Aggregator, listeners []StepListener, partitionListeners []PartitionListener) *partitionStep {
	return &partitionStep{
		name:               step.Name(),
		step:               step,
		partitions:         partitions,
		partitioner:        partitioner,
		aggregator:         aggregator,
		listeners:          listeners,
		partitionListeners: partitionListeners,
		taskPool:           stepPool,
	}
}

func (step *partitionStep) Name() string {
	return step.name
}

func (step *partitionStep) Exec(ctx context.Context, execution *StepExecution) (err BatchError) {
	defer func() {
		err = execEnd(ctx, execution, err, recover())
	}()
	logger.Info(ctx, "start executing step, jobExecutionId:%v, stepName:%v", execution.JobExecution.JobExecutionId, execution.StepName)
	for _, listener := range step.listeners {
		err = listener.BeforeStep(execution)
		if err != nil {
			logger.Error(ctx, "step listener executing error, jobExecutionId:%v, stepName:%v, listener:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, reflect.TypeOf(listener).String(), err)
			return err
		}
	}
	execution.start()
	e := saveStepExecution(ctx, execution)
	if e != nil {
		err = e
		logger.Error(ctx, "save step execution failed, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, e)
		return err
	}
	for _, listener := range step.partitionListeners {
		err = listener.BeforePartition(execution)
		if err != nil {
			logger.Error(ctx, "partition listener executing error, jobExecutionId:%v, stepName:%v, listener:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, reflect.TypeOf(listener).String(), err)
			return err
		}
	}
	subExecutions, err := step.split(ctx, execution, step.partitions)
	if err != nil {
		logger.Error(ctx, "step split error, jobExecutionId:%v, stepName:%v", execution.JobExecution.JobExecutionId, execution.StepName, err)
		for _, listener := range step.partitionListeners {
			listener.OnError(execution, err)
		}
		return err
	}
	logger.Info(ctx, "step:%v splitted into %d partitions", execution.StepName, len(subExecutions))
	for _, listener := range step.partitionListeners {
		err = listener.AfterPartition(execution, subExecutions)
		if err != nil {
			logger.Error(ctx, "partition listener executing error, jobExecutionId:%v, stepName:%v, listener:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, reflect.TypeOf(listener).String(), err)
			return err
		}
	}
	e = saveStepExecution(ctx, execution)
	if e != nil {
		err = e
		logger.Error(ctx, "save step execution failed, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, e)
		return err
	}
	for _, subExecution := range subExecutions {
		e = saveStepExecution(ctx, subExecution)
		if e != nil {
			err = e
			logger.Error(ctx, "save sub-step execution failed, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, subExecution.StepName, e)
			return err
		}
	}
	futures := make([]Future, 0, len(subExecutions))
	for _, subExecution := range subExecutions {
		task := func(e *StepExecution) func() (interface{}, error) {
			return func() (interface{}, error) {
				logger.Info(ctx, "sub-step execute start, jobExecutionId:%v, sub-step name:%v", e.JobExecution.JobExecutionId, e.StepName)
				err = step.step.Exec(ctx, e)
				return nil, err
			}
		}(subExecution)
		fu := step.taskPool.Submit(ctx, task)
		futures = append(futures, fu)
	}
	stepStatus := status.COMPLETED
	for i, fu := range futures {
		_, err2 := fu.Get()
		if err2 != nil {
			logger.Error(ctx, "sub-step execute failed, jobExecutionId:%v, sub-step name:%v, err:%v", subExecutions[i].JobExecution.JobExecutionId, subExecutions[i].StepName, err2)
			if subExecutions[i].StepStatus != status.FAILED {
				subExecutions[i].finish(NewBatchError(ErrCodeGeneral, "sub-step execution error", err2))
			}
		} else {
			if subExecutions[i].StepStatus == status.STARTED {
				subExecutions[i].finish(nil)
			}
			logger.Info(ctx, "sub-step execute finish, jobExecutionId:%v, sub-step name:%v, sub-step status:%v", subExecutions[i].JobExecution.JobExecutionId, subExecutions[i].StepName, subExecutions[i].StepStatus)
		}
		e := saveStepExecution(ctx, subExecutions[i])
		if e != nil {
			err = e
			logger.Error(ctx, "save sub-step execution failed, jobExecutionId:%v, stepName:%v, err:%v", subExecutions[i].JobExecution.JobExecutionId, subExecutions[i].StepName, e)
		}
		stepStatus = stepStatus.And(subExecutions[i].StepStatus)
	}
	//aggregate
	if stepStatus == status.COMPLETED && step.aggregator != nil {
		partitionNames := step.partitioner.GetPartitionNames(execution, step.partitions)
		allSubExecutions := make([]*StepExecution, 0)
		for _, partitionName := range partitionNames {
			subExecution, err := findLastCompleteStepExecution(execution.JobExecution.JobInstanceId, partitionName)
			if err != nil {
				return err
			}
			allSubExecutions = append(allSubExecutions, subExecution)
		}
		err = step.aggregator.Aggregate(execution, allSubExecutions)
		if err != nil {
			stepStatus = status.FAILED
			logger.Error(ctx, "aggregate sub-step error, jobExecutionId:%v, stepName:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, err)
		} else {
			logger.Info(ctx, "aggregate sub-step finish, jobExecutionId:%v, stepName:%v", execution.JobExecution.JobExecutionId, execution.StepName)
		}
	}
	execution.StepStatus = stepStatus
	execution.EndTime = time.Now()
	for _, listener := range step.listeners {
		err = listener.AfterStep(execution)
		if err != nil {
			logger.Error(ctx, "step listener executing error, jobExecutionId:%v, stepName:%v, listener:%v, err:%v", execution.JobExecution.JobExecutionId, execution.StepName, reflect.TypeOf(listener).String(), err)
			break
		}
	}
	logger.Info(ctx, "finish step execution, jobExecutionId:%v, stepName:%v, stepStatus:%v", execution.JobExecution.JobExecutionId, execution.StepName, execution.StepStatus)
	return nil
}

const partitionStepPartitionsKey = "gobatch.partitionStep.partitions"

func (step *partitionStep) split(ctx context.Context, execution *StepExecution, partitions uint) ([]*StepExecution, BatchError) {
	if execution.StepExecutionContext.Exists(partitionStepPartitionsKey) {
		savedPartitions, e := execution.StepExecutionContext.GetInt(partitionStepPartitionsKey, int(partitions))
		if e != nil {
			return nil, NewBatchError(ErrCodeGeneral, "get '%v' from step:%v execution context failed", partitionStepPartitionsKey, execution.StepName, e)
		}
		logger.Info(ctx, "split step:%v, saved partitions in StepExecutionContext:%v, try to get last splitted subExecutions", execution.StepName, savedPartitions)
		subExecutions := make([]*StepExecution, 0, savedPartitions)
		partitionNames := step.partitioner.GetPartitionNames(execution, uint(savedPartitions))
		missingPartition := false
		for _, partitionName := range partitionNames {
			lastStepExecution, err := findLastStepExecution(execution.JobExecution.JobInstanceId, partitionName)
			if err != nil {
				return nil, err
			}
			if lastStepExecution != nil {
				if util.In(lastStepExecution.StepStatus, []interface{}{status.STARTING, status.STARTED, status.STOPPING, status.UNKNOWN}) {
					logger.Error(ctx, "last StepExecution is in progress or terminated abnormally, jobExecutionId:%v, stepName:%v", execution.JobExecution.JobExecutionId, execution.StepName)
					return nil, NewBatchError(ErrCodeGeneral, "last StepExecution is in progress or terminated abnormally, jobExecutionId:%v, stepName:%v", execution.JobExecution.JobExecutionId, execution.StepName)
				}
				if lastStepExecution.StepStatus == status.COMPLETED {
					continue
				}
				if lastStepExecution.StepContextId != 0 {
					subExecution := lastStepExecution.deepCopy()
					subExecution.StepContextId = lastStepExecution.StepContextId
					//subExecution.StepName = partitionName
					//subExecution.StepContext.Put(ItemReaderKeyList, lastStepExecution.StepContext.Get(ItemReaderKeyList))
					//subExecution.StepExecutionContext.Put(ItemReaderCurrentIndex, lastStepExecution.StepExecutionContext.Get(ItemReaderCurrentIndex))
					//subExecution.StepExecutionContext.Put(ItemReaderMaxIndex, lastStepExecution.StepExecutionContext.Get(ItemReaderMaxIndex))
					subExecution.JobExecution = execution.JobExecution
					subExecutions = append(subExecutions, subExecution)
				} else {
					missingPartition = true
					break
				}
			} else {
				missingPartition = true
				break
			}
		}
		if !missingPartition {
			return subExecutions, nil
		}
	}
	subExecutions, err := step.partitioner.Partition(execution, partitions)
	if err != nil {
		return nil, err
	}
	for _, subExecution := range subExecutions {
		subExecution.JobExecution = execution.JobExecution
	}
	execution.StepExecutionContext.Put(partitionStepPartitionsKey, len(subExecutions))
	return subExecutions, nil
}

func (step *partitionStep) addListener(listener StepListener) {
	step.listeners = append(step.listeners, listener)
}

func (step *partitionStep) addPartitionListener(listener PartitionListener) {
	step.partitionListeners = append(step.partitionListeners, listener)
}
