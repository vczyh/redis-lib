package rdb

import (
	"fmt"
	"strconv"
)

type ZSetObjectEvent struct {
	Key     string
	Members []ZSetMember
}

type ZSetMember struct {
	Value string
	Score float64
}

func (e *ZSetObjectEvent) Debug() {
	fmt.Printf("=== ZSetObjectEvent ===\n")
	fmt.Printf("Key: %s\n", e.Key)
	fmt.Printf("Size: %d\n", len(e.Members))
	fmt.Printf("Members:\n")
	for _, member := range e.Members {
		fmt.Printf("\t%s %f\n", member.Value, member.Score)
	}
	fmt.Printf("\n")
}

func parseZSet(key string, r *rdbReader, valueType byte) (*ZSetObjectEvent, error) {
	zSet := &ZSetObjectEvent{Key: key}
	switch valueType {
	case rdbTypeZSetZipList:
		return parseZSetInZipList(r, zSet)
	case rdbTypeZSetListPack:
		return parseZSetInListPack(r, zSet)
	case rdbTypeZSet:
		return parseZSet0(r, zSet)
	case rdbTypeZSet2:
		return parseZSet2(r, zSet)
	default:
		return nil, fmt.Errorf("unsupported zset value type: %x", valueType)
	}
}

func parseZSet0(r *rdbReader, set *ZSetObjectEvent) (*ZSetObjectEvent, error) {
	length, err := r.GetLengthInt()
	if err != nil {
		return nil, err
	}
	members := make([]ZSetMember, length)
	for i := 0; i < length; i++ {
		value, err := r.GetLengthString()
		if err != nil {
			return nil, err
		}
		score, err := r.GetDoubleValue()
		if err != nil {
			return nil, err
		}
		members[i].Value = value
		members[i].Score = score
	}
	set.Members = members

	return set, nil
}

func parseZSet2(r *rdbReader, set *ZSetObjectEvent) (*ZSetObjectEvent, error) {
	length, err := r.GetLengthInt()
	if err != nil {
		return nil, err
	}
	members := make([]ZSetMember, length)
	for i := 0; i < length; i++ {
		value, err := r.GetLengthString()
		if err != nil {
			return nil, err
		}
		score, err := r.GetLDouble()
		if err != nil {
			return nil, err
		}
		members[i].Value = value
		members[i].Score = score
	}
	set.Members = members

	return set, nil
}

func parseZSetInZipList(r *rdbReader, set *ZSetObjectEvent) (*ZSetObjectEvent, error) {
	list, err := parseZipList(r)
	if err != nil {
		return nil, err
	}

	if len(list)%2 != 0 {
		return nil, fmt.Errorf("error length for ziplist: %d", len(list))
	}

	members := make([]ZSetMember, len(list)/2)
	for i := 0; i < len(list); i += 2 {
		value := list[i]
		score := list[i+1]
		scoreDouble, err := strconv.ParseFloat(score, 10)
		if err != nil {
			return nil, err
		}
		members[i/2].Value = value
		members[i/2].Score = scoreDouble
	}
	set.Members = members

	return set, nil
}

func parseZSetInListPack(r *rdbReader, set *ZSetObjectEvent) (*ZSetObjectEvent, error) {
	list, err := parseListPack(r)
	if err != nil {
		return nil, err
	}

	if len(list)%2 != 0 {
		return nil, fmt.Errorf("error length for listpack: %d", len(list))
	}

	members := make([]ZSetMember, len(list)/2)
	for i := 0; i < len(list); i += 2 {
		value := list[i]
		score := list[i+1]
		scoreDouble, err := strconv.ParseFloat(score, 10)
		if err != nil {
			return nil, err
		}
		members[i/2].Value = value
		members[i/2].Score = scoreDouble
	}
	set.Members = members

	return set, nil
}
