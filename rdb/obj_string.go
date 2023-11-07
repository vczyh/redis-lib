package rdb

import "fmt"

type StringObjectEvent struct {
	Key   string
	Value string
}

func parseString(key string, r *rdbReader) (*StringObjectEvent, error) {
	value, err := r.GetLengthString()
	if err != nil {
		return nil, err
	}
	return &StringObjectEvent{
		Key:   key,
		Value: value,
	}, nil
}

func (e *StringObjectEvent) Debug() {
	fmt.Printf("=== StringObjectEvent ===\n")
	fmt.Printf("Key: %s\n", e.Key)
	fmt.Printf("Value: %s\n", e.Value)
	fmt.Printf("\n")
}
