package rdb

import (
	"fmt"
)

const (
	zipStrMask = 0xc0
	zipStr06B  = 0 << 6
	zipStr14B  = 1 << 6
	zipStr32B  = 2 << 6

	zipIntMask    = 0x30
	zipInt8B      = 0xfe
	zipInt16B     = 0xc0 | 0<<4
	zipInt32B     = 0xc0 | 1<<4
	zipInt64B     = 0xc0 | 2<<4
	zipInt24B     = 0xc0 | 3<<4
	zipIntImmMask = 0x0f
	zipIntImmMin  = 0xf1
	zipIntImmMax  = 0xfd
)

type ListObjectEvent struct {
	Key     string
	Members []string
}

func (e *ListObjectEvent) Debug() {
	fmt.Printf("=== ListObjectEvent ===\n")
	fmt.Printf("Key: %s\n", e.Key)
	fmt.Printf("Size: %d\n", len(e.Members))
	fmt.Printf("Members:\n")
	for _, member := range e.Members {
		fmt.Printf("\t%s\n", member)
	}
	fmt.Printf("\n")
}

func parseList(key string, r *rdbReader, valueType byte) (*ListObjectEvent, error) {
	list := &ListObjectEvent{
		Key: key,
	}
	switch valueType {
	case valueTypeList:
		// TODO not tested
		return parseList0(r, list)
	case valueTypeZipList:
		// TODO not tested
		return parseListInZipList(r, list)
	case valueTypeListQuickList:
		return parseListInQuickList(r, list)
	default:
		return nil, fmt.Errorf("unsupported list value type: %x", valueType)
	}
}

func parseList0(r *rdbReader, list *ListObjectEvent) (*ListObjectEvent, error) {
	size, err := r.GetLengthInt()
	if err != nil {
		return nil, err
	}
	members := make([]string, size)
	for i := 0; i < size; i++ {
		item, err := r.GetLengthString()
		if err != nil {
			return nil, err
		}
		members[i] = item
	}
	list.Members = members
	return list, nil
}

func parseListInZipList(r *rdbReader, list *ListObjectEvent) (*ListObjectEvent, error) {
	zipList, err := parseZipList(r)
	if err != nil {
		return nil, err
	}
	list.Members = zipList
	return list, nil
}

func parseListInQuickList(r *rdbReader, list *ListObjectEvent) (*ListObjectEvent, error) {
	size, err := r.GetLengthInt()
	if err != nil {
		return nil, err
	}
	var members []string
	for i := 0; i < size; i++ {
		zipList, err := parseZipList(r)
		if err != nil {
			return nil, err
		}
		members = append(members, zipList...)
	}
	list.Members = members
	return list, nil
}
