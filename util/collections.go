package util

// Contains if the slice contains the element
func Contains(set []interface{}, val interface{}) bool {
	for _, v := range set {
		if v == val {
			return true
		}
	}
	return false
}

// In if the element in the slice
func In(val interface{}, set []interface{}) bool {
	return Contains(set, val)
}
