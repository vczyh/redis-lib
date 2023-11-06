package rdb

import (
	"fmt"
)

type Set struct {
	Key     string
	Members []string
}

func (e *Set) Debug() {
	fmt.Printf("=== Set ===\n")
	fmt.Printf("Key: %s\n", e.Key)
	fmt.Printf("Size: %d\n", len(e.Members))
	fmt.Printf("Members:\n")
	for _, member := range e.Members {
		fmt.Printf("\t%s\n", member)
	}
	fmt.Printf("\n")
}

func parseSet(key string, r *Reader) (*Set, error) {
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
	return &Set{Key: key, Members: members}, nil
}
