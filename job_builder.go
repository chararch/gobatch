package gobatch

import "fmt"

type jobBuilder struct {
	name               string
	steps              []Step
	jobListeners       []JobListener
	stepListeners      []StepListener
	chunkListeners     []ChunkListener
	partitionListeners []PartitionListener
}

//NewJob new instance of job builder
func NewJob(name string, steps ...Step) *jobBuilder {
	if name == "" {
		panic("job name must not be empty")
	}
	builder := &jobBuilder{
		name:  name,
		steps: steps,
	}
	return builder
}

func (builder *jobBuilder) Step(step ...Step) *jobBuilder {
	builder.steps = append(builder.steps, step...)
	return builder
}

func (builder *jobBuilder) Listener(listener ...interface{}) *jobBuilder {
	for _, l := range listener {
		switch ll := l.(type) {
		case JobListener:
			builder.jobListeners = append(builder.jobListeners, ll)
		case StepListener:
			builder.stepListeners = append(builder.stepListeners, ll)
		case ChunkListener:
			builder.chunkListeners = append(builder.chunkListeners, ll)
		case PartitionListener:
			builder.partitionListeners = append(builder.partitionListeners, ll)
		default:
			panic(fmt.Sprintf("not supported listener:%+v for job:%v", ll, builder.name))
		}
	}
	return builder
}

func (builder *jobBuilder) Build() Job {
	var job Job
	for _, sl := range builder.stepListeners {
		for _, step := range builder.steps {
			step.addListener(sl)
		}
	}
	for _, cl := range builder.chunkListeners {
		for _, step := range builder.steps {
			if chkStep, ok := step.(*chunkStep); ok {
				chkStep.addChunkListener(cl)
			}
		}
	}
	for _, pl := range builder.partitionListeners {
		for _, step := range builder.steps {
			if chkStep, ok := step.(*partitionStep); ok {
				chkStep.addPartitionListener(pl)
			}
		}
	}
	job = newSimpleJob(builder.name, builder.steps, builder.jobListeners)
	return job
}
