package rdb

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseZSetWithListPack(t *testing.T) {
	b := []byte{0x08, 0x6B, 0x65, 0x79, 0x5F, 0x7A,
		0x73, 0x65, 0x74, 0x18, 0x18, 0x00, 0x00, 0x00, 0x04, 0x00, 0x81,
		0x61, 0x02, 0x83, 0x30, 0x2E, 0x32, 0x04, 0x81, 0x62, 0x02, 0x84, 0x30, 0x2E,
		0x34, 0x35, 0x05, 0xFF,
	}
	r := newRdbReader(bytes.NewReader(b))

	key, err := r.GetLengthString()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "key_zset", key)

	e, err := parseZSet(key, r, valueTypeZSetListPack)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(e.Members))

	assert.Equal(t, "a", e.Members[0].Value)
	assert.Equal(t, 0.2, e.Members[0].Score)

	assert.Equal(t, "b", e.Members[1].Value)
	assert.Equal(t, 0.45, e.Members[1].Score)
}

func TestParseZSetWithZSet2(t *testing.T) {
	b := []byte{0x08, 0x6B, 0x65, 0x79, 0x5F, 0x7A, 0x73, 0x65, 0x74,
		0x02, 0x01, 0x62, 0xCD, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xDC,
		0x3F, 0x01, 0x61, 0x9A, 0x99, 0x99, 0x99, 0x99, 0x99, 0xC9, 0x3F,
	}
	r := newRdbReader(bytes.NewReader(b))

	key, err := r.GetLengthString()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "key_zset", key)

	e, err := parseZSet(key, r, valueTypeZSet2)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(e.Members))

	assert.Equal(t, "b", e.Members[0].Value)
	assert.Equal(t, 0.45, e.Members[0].Score)

	assert.Equal(t, "a", e.Members[1].Value)
	assert.Equal(t, 0.2, e.Members[1].Score)
}
