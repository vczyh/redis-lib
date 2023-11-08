package rdb

import (
	"testing"
)

func TestParser_Parse(t *testing.T) {
	//p, err := NewParser("/tmp/test222.rdb")
	p, err := NewParser("/Users/zhangyuheng/workspace/mine/container-images/redis/redis/data/source/dump.rdb")
	if err != nil {
		t.Fatal(err)
	}

	s, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}

	for s.HasNext() {
		e := s.Next()
		e.Event.Debug()
	}

	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
}
