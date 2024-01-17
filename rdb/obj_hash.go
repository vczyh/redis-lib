package rdb

import (
	"fmt"
)

type HashObjectEvent struct {
	Key    string
	Fields []HashField
}

type HashField struct {
	Field string
	Value string
}

func (e *HashObjectEvent) Debug() {
	fmt.Printf("=== HashObjectEvent ===\n")
	fmt.Printf("Key: %s\n", e.Key)
	fmt.Printf("Size: %d\n", len(e.Fields))
	fmt.Printf("Fields:\n")
	for i := range e.Fields {
		field := e.Fields[i]
		fmt.Printf("\t%s = %s\n", field.Field, field.Value)
	}
	fmt.Printf("\n")
}

func parseHash(key string, r *rdbReader, valueType byte) (*HashObjectEvent, error) {
	h := &HashObjectEvent{Key: key}
	switch valueType {
	case valueTypeHashZipList:
		return parseHashInZipList(r, h)
	case valueTypeHashListPack:
		return parseHashInListPack(r, h)
	case valueTypeHash:
		return parseHash0(r, h)
	default:
		return nil, fmt.Errorf("unsupported hash value type: %x", valueType)
	}
}

func parseHash0(r *rdbReader, h *HashObjectEvent) (*HashObjectEvent, error) {
	dictSize, err := r.GetLengthInt()
	if err != nil {
		return nil, err
	}
	fields := make([]HashField, dictSize)
	for i := 0; i < dictSize; i++ {
		field, err := r.GetLengthString()
		if err != nil {
			return nil, err
		}
		value, err := r.GetLengthString()
		if err != nil {
			return nil, err
		}
		fields[i] = HashField{
			Field: field,
			Value: value,
		}
	}
	h.Fields = fields

	return h, err
}

func parseHashInZipList(r *rdbReader, h *HashObjectEvent) (*HashObjectEvent, error) {
	list, err := parseZipList(r)
	if err != nil {
		return nil, err
	}
	if len(list)%2 != 0 {
		return nil, fmt.Errorf("error length for ziplist: %d", len(list))
	}

	fields := make([]HashField, len(list)/2)
	for i := 0; i < len(list)/2; i++ {
		field := list[i*2]
		value := list[i*2+1]
		fields[i] = HashField{
			Field: field,
			Value: value,
		}
	}
	h.Fields = fields

	return h, nil
}

func parseHashInListPack(r *rdbReader, h *HashObjectEvent) (*HashObjectEvent, error) {
	list, err := parseListPack(r)
	if err != nil {
		return nil, err
	}
	if len(list)%2 != 0 {
		return nil, fmt.Errorf("error length for ziplist: %d", len(list))
	}

	fields := make([]HashField, len(list)/2)
	for i := 0; i < len(list)/2; i++ {
		field := list[i*2]
		value := list[i*2+1]
		fields[i] = HashField{
			Field: field,
			Value: value,
		}
	}
	h.Fields = fields

	return h, nil
}
