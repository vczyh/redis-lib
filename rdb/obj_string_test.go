package rdb

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseString(t *testing.T) {
	b := []byte{0x0A, 0x6B, 0x65, 0x79, 0x5F, 0x73, 0x74, 0x72, 0x69, 0x6E, 0x67, 0x04, 0x61, 0x61, 0x61, 0x61}
	r := newRdbReader(bytes.NewReader(b))

	key, err := r.GetLengthString()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "key_string", key)

	e, err := parseString(key, r)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "aaaa", e.Value)
}
