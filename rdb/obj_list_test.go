package rdb

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseListWithListQuickList2(t *testing.T) {
	b := []byte{0x08, 0x6B, 0x65, 0x79, 0x5F, 0x6C, 0x69, 0x73, 0x74,
		0x01, 0x02, 0x11, 0x11, 0x00, 0x00, 0x00, 0x02, 0x00, 0x83, 0x62, 0x61, 0x72, 0x04, 0x83, 0x66, 0x6F, 0x6F, 0x04, 0xFF,
	}
	r := newRdbReader(bytes.NewReader(b))

	key, err := r.GetLengthString()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "key_list", key)

	e, err := parseList(key, r, valueTypeListQuickList2)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "bar", e.Members[0])
	assert.Equal(t, "foo", e.Members[1])
}
