package rdb

import "fmt"

type StringObjectEvent struct {
	RedisKey

	Value string
}

func parseString(key RedisKey, r *rdbReader) (*StringObjectEvent, error) {
	value, err := r.GetLengthString()
	if err != nil {
		return nil, err
	}
	return &StringObjectEvent{
		RedisKey: key,
		Value:    value,
	}, nil
}

func (e *StringObjectEvent) Debug() {
	fmt.Printf("=== StringObjectEvent ===\n")
	e.debugKey()
	fmt.Printf("Value: %s\n", e.Value)
	fmt.Printf("\n")
}
