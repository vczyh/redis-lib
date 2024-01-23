package rdb

import (
	"fmt"
)

type ListObjectEvent struct {
	RedisKey

	Elements []string
}

func (e *ListObjectEvent) Debug() {
	fmt.Printf("=== ListObjectEvent ===\n")
	e.debugKey()
	fmt.Printf("Size: %d\n", len(e.Elements))
	fmt.Printf("Elements:\n")
	for _, ele := range e.Elements {
		fmt.Printf("\t%s\n", ele)
	}
	fmt.Printf("\n")
}

func parseList(key RedisKey, r *rdbReader, valueType byte) (*ListObjectEvent, error) {
	list := &ListObjectEvent{
		RedisKey: key,
	}
	switch valueType {
	case rdbTypeList:
		// TODO not tested
		return parseList0(r, list)
	case rdbTypeZipList:
		// TODO not tested
		return parseListInZipList(r, list)
	case rdbTypeListQuickList:
		return parseListInQuickList(r, list)
	case rdbTypeListQuickList2:
		return parseListInQuickList2(r, list)
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
	list.Elements = members
	return list, nil
}

func parseListInZipList(r *rdbReader, list *ListObjectEvent) (*ListObjectEvent, error) {
	zipList, err := parseZipList(r)
	if err != nil {
		return nil, err
	}
	list.Elements = zipList
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
	list.Elements = members
	return list, nil
}

func parseListInQuickList2(r *rdbReader, list *ListObjectEvent) (*ListObjectEvent, error) {
	size, err := r.GetLengthInt()
	if err != nil {
		return nil, err
	}
	for i := 0; i < size; i++ {
		container, err := r.GetLengthInt()
		if err != nil {
			return nil, err
		}

		switch container {
		case 1:
			member, err := r.GetLengthString()
			if err != nil {
				return nil, err
			}
			list.Elements = append(list.Elements, member)
		case 2:
			members, err := parseListPack(r)
			if err != nil {
				return nil, err
			}
			list.Elements = append(list.Elements, members...)
		default:
			return nil, fmt.Errorf("quicklist integrity check failed, unsupported listpack container: %d", container)
		}
	}

	return list, err
}
