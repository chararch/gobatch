package status

type BatchStatus string

const (
	STARTING  BatchStatus = "STARTING"
	STARTED   BatchStatus = "STARTED"
	STOPPING  BatchStatus = "STOPPING"
	STOPPED   BatchStatus = "STOPPED"
	COMPLETED BatchStatus = "COMPLETED"
	FAILED    BatchStatus = "FAILED"
	UNKNOWN   BatchStatus = "UNKNOWN"
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
