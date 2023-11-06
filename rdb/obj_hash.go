package rdb

import "fmt"

type Hash struct {
	Key    string
	Fields []HashField
}

type HashField struct {
	Field string
	Value string
}

func (e *Hash) Debug() {
	fmt.Printf("=== Hash ===\n")
	fmt.Printf("Key: %s\n", e.Key)
	fmt.Printf("Size: %d\n", len(e.Fields))
	fmt.Printf("Fields:\n")
	for i := range e.Fields {
		field := e.Fields[i]
		fmt.Printf("\t%s = %s\n", field.Field, field.Value)
	}
	fmt.Printf("\n")
}

func parseHash(key string, r *Reader, valueType byte) (*Hash, error) {
	h := &Hash{Key: key}
	switch valueType {
	case ValueTypeHashZipList:
		return parseHashInZipList(r, h)
	default:
		return nil, fmt.Errorf("unsupported hash value type: %x", valueType)
	}
}

func parseHashInZipList(r *Reader, h *Hash) (*Hash, error) {
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
