package rdb

import (
	"fmt"
	"strconv"
)

type SetObjectEvent struct {
	Key     string
	Members []string
}

func (e *SetObjectEvent) Debug() {
	fmt.Printf("=== SetObjectEvent ===\n")
	fmt.Printf("Key: %s\n", e.Key)
	fmt.Printf("Size: %d\n", len(e.Members))
	fmt.Printf("Members:\n")
	for _, member := range e.Members {
		fmt.Printf("\t%s\n", member)
	}
	fmt.Printf("\n")
}

func parseSet(key string, r *rdbReader, valueType byte) (*SetObjectEvent, error) {
	set := &SetObjectEvent{Key: key}
	switch valueType {
	case valueTypeSet:
		return parseSet0(r, set)
	case valueTypeSetListPack:
		return parseSetInListPack(r, set)
	case valueTypeIntSet:
		return parseSetInIntSet(r, set)
	default:
		return nil, fmt.Errorf("unsupported set value type: %x", valueType)
	}
}

func parseSet0(r *rdbReader, set *SetObjectEvent) (*SetObjectEvent, error) {
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
	set.Members = members

	return set, nil
}

func parseSetInListPack(r *rdbReader, set *SetObjectEvent) (*SetObjectEvent, error) {
	list, err := parseListPack(r)
	if err != nil {
		return nil, err
	}
	set.Members = append(set.Members, list...)
	return set, nil
}

func parseSetInIntSet(r *rdbReader, set *SetObjectEvent) (*SetObjectEvent, error) {
	members, err := parseIntSet(r)
	if err != nil {
		return nil, err
	}
	elements := make([]string, len(members))
	for i, member := range members {
		elements[i] = strconv.FormatInt(member, 10)
	}
	set.Members = elements

	return set, nil
}
