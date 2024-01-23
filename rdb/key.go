package rdb

import (
	"fmt"
	"time"
)

type RedisKey struct {
	DbId int

	Key string

	// millisecond
	expireAt int64
}

func (k RedisKey) debugKey() {
	fmt.Printf("DbId: %d\n", k.DbId)
	fmt.Printf("Key: %s\n", k.Key)
	if k.expireAt != -1 {
		fmt.Printf("Expire At: %s\n", time.UnixMicro(k.expireAt*1e3))
	}
}
