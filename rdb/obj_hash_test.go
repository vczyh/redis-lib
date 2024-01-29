package rdb

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseHashWithListPack(t *testing.T) {
	b := []byte{0x08, 0x6B, 0x65, 0x79, 0x5F, 0x68, 0x61, 0x73, 0x68, 0x1D, 0x1D, 0x00, 0x00, 0x00,
		0x04, 0x00, 0x84, 0x6B, 0x65, 0x79, 0x31, 0x05, 0x83, 0x66,
		0x6F, 0x6F, 0x04, 0x84, 0x6B, 0x65, 0x79, 0x32, 0x05, 0x83, 0x62, 0x61, 0x72, 0x04, 0xFF,
	}
	r := newRdbReader(bytes.NewReader(b))

	key, err := r.GetLengthString()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "key_hash", key)

	e, err := parseHash(RedisKey{}, r, rdbTypeHashListPack)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range e.Fields {
		switch field.Field {
		case "key1":
			assert.Equal(t, "foo", field.Value)
		case "key2":
			assert.Equal(t, "bar", field.Value)
		default:
			t.Fatal("hash container unexpected field")
		}
	}
}

func TestParseHashWithHash(t *testing.T) {
	b := []byte{0x08, 0x6B, 0x65, 0x79, 0x5F, 0x68, 0x61, 0x73, 0x68, 0x02, 0x04, 0x6B, 0x65, 0x79,
		0x32, 0x03, 0x62, 0x61, 0x72, 0x04, 0x6B, 0x65, 0x79, 0x31, 0x03, 0x66, 0x6F, 0x6F,
	}
	r := newRdbReader(bytes.NewReader(b))

	key, err := r.GetLengthString()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "key_hash", key)

	e, err := parseHash(RedisKey{}, r, rdbTypeHash)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range e.Fields {
		switch field.Field {
		case "key1":
			assert.Equal(t, "foo", field.Value)
		case "key2":
			assert.Equal(t, "bar", field.Value)
		default:
			t.Fatal("hash container unexpected field")
		}
	}
}
