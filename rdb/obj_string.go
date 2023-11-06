package rdb

import "fmt"

type StringEvent struct {
	Key   string
	Value string
}

func parseString(key string, r *Reader) (*StringEvent, error) {
	value, err := r.GetLengthString()
	if err != nil {
		return nil, err
	}
	return &StringEvent{
		Key:   key,
		Value: value,
	}, nil
}

func (e *StringEvent) Debug() {
	fmt.Printf("=== StringEvent ===\n")
	fmt.Printf("Key: %s\n", e.Key)
	fmt.Printf("Value: %s\n", e.Value)
	fmt.Printf("\n")
}
