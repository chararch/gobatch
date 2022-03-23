package status

//BatchStatus status of job or step execution
type BatchStatus string

const (
	//STARTING represent beginning of a job or step execution
	STARTING BatchStatus = "STARTING"
	//STARTED job or step have be started and is running
	STARTED BatchStatus = "STARTED"
	//STOPPING job or step to be stopped
	STOPPING BatchStatus = "STOPPING"
	//STOPPED job or step have be stopped
	STOPPED BatchStatus = "STOPPED"
	//COMPLETED job or step have finished successfully
	COMPLETED BatchStatus = "COMPLETED"
	//FAILED job or step have failed
	FAILED BatchStatus = "FAILED"
	//UNKNOWN job or step have aborted due to unknown reason
	UNKNOWN BatchStatus = "UNKNOWN"
)

var statuses = map[BatchStatus]int{
	STARTING:  0,
	STARTED:   1,
	STOPPING:  2,
	STOPPED:   3,
	COMPLETED: 4,
	FAILED:    5,
	UNKNOWN:   6,
}

func (s BatchStatus) And(other BatchStatus) BatchStatus {
	i1, ok1 := statuses[s]
	i2, ok2 := statuses[other]
	if ok1 && ok2 {
		if i1 < i2 {
			return other
		} else {
			return s
		}
	} else if ok1 {
		return other
	} else {
		return s
	}
}
