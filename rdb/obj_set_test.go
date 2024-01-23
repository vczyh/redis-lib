package rdb

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseSetWithListPack(t *testing.T) {
	b := []byte{
		0x07, 0x6B, 0x65, 0x79, 0x5F, 0x73, 0x65, 0x74, 0x0D, 0x0D, 0x00, 0x00, 0x00, 0x02, 0x00, 0x81, 0x61, 0x02, 0x81, 0x62, 0x02, 0xFF,
	}
	r := newRdbReader(bytes.NewReader(b))

	key, err := r.GetLengthString()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "key_set", key)

	e, err := parseSet(RedisKey{}, r, rdbTypeSetListPack)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(e.Members))
	assert.Contains(t, e.Members, "a")
	assert.Contains(t, e.Members, "b")
}

func TestParseSetWithIntSet(t *testing.T) {
	b := []byte{0x07, 0x6B, 0x65, 0x79, 0x5F, 0x73, 0x65, 0x74, 0x0C, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x64, 0x00, 0xC8, 0x00}
	r := newRdbReader(bytes.NewReader(b))

	key, err := r.GetLengthString()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "key_set", key)

	e, err := parseSet(RedisKey{}, r, rdbTypeIntSet)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(e.Members))
	assert.Contains(t, e.Members, "100")
	assert.Contains(t, e.Members, "200")
}
