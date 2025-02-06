package splice

func Contains[T any](array []T, element T) bool {
	for _, v := range array {
		if any(v) == any(element) {
			return true
		}
	}
	return false
}
