package main

import (
	"github.com/vczyh/redis-lib/rdb"
)

func main() {
	p, err := rdb.NewParser("/tmp/rdb_test.rdb")
	if err != nil {
		panic(err)
	}

	s, err := p.Parse()
	if err != nil {
		panic(err)
	}

	for s.HasNext() {
		e := s.Next()

		switch e.EventType {
		case rdb.EventTypeVersion:
			e.Event.Debug()
		case rdb.EventTypeStringObject:
			e.Event.Debug()
		case rdb.EventTypeSetObject:
			e.Event.Debug()
		}
	}

	if err := s.Err(); err != nil {
		panic(err)
	}
}
