package util

func Contains(set []interface{}, val interface{}) bool {
	for _, v := range set {
		if v == val {
			return true
		}
	}
	return false
}

func In(val interface{}, set []interface{}) bool {
	return Contains(set, val)
}
