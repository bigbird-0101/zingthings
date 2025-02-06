package env

import (
	"os"
	"strconv"
)

// GetEnvOrDefault 定义泛型方法获取 环境变量的值，如果环境变量为空则返回默认值
func GetEnvOrDefault[T any](key string, defaultValue T) T {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	// 根据默认值的类型进行转换
	var result T
	switch any(defaultValue).(type) {
	case string:
		result = any(value).(T)
	case int:
		if intValue, err := strconv.Atoi(value); err == nil {
			result = any(intValue).(T)
		} else {
			result = defaultValue
		}
	case bool:
		if boolValue, err := strconv.ParseBool(value); err == nil {
			result = any(boolValue).(T)
		} else {
			result = defaultValue
		}
	// 可以根据需要添加更多的类型转换
	default:
		result = defaultValue
	}
	return result
}
