package rdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
)

const (
	zipStrMask = 0xc0
	zipStr06B  = 0 << 6
	zipStr14B  = 1 << 6
	zipStr32B  = 2 << 6

	zipIntMask    = 0x30
	zipInt8B      = 0xfe
	zipInt16B     = 0xc0 | 0<<4
	zipInt32B     = 0xc0 | 1<<4
	zipInt64B     = 0xc0 | 2<<4
	zipInt24B     = 0xc0 | 3<<4
	zipIntImmMask = 0x0f
	zipIntImmMin  = 0xf1
	zipIntImmMax  = 0xfd
)

func parseZipList(r *rdbReader) ([]string, error) {
	zipBytes, err := r.GetLengthBytes()
	if err != nil {
		return nil, err
	}
	r = newRdbReader(bytes.NewReader(zipBytes))

	_, err = r.GetLUint32()
	if err != nil {
		return nil, err
	}

	_, err = r.GetLUint32()
	if err != nil {
		return nil, err
	}

	zlLen, err := r.GetLUint16()
	if err != nil {
		return nil, err
	}
	var members []string
	for i := 0; i < int(zlLen); i++ {
		entryValue, err := parseZipListEntry(r)
		if err != nil {
			return nil, err
		}
		members = append(members, entryValue)
	}

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if b != 0xff {
		return nil, fmt.Errorf("list should EOF")
	}

	return members, nil
}

// ziplist.c::ZIP_DECODE_LENGTH
func parseZipListEntry(r *rdbReader) (string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	preEntryLen := int(b)
	if b == 254 {
		u, err := r.GetLUint32()
		if err != nil {
			return "", err
		}
		preEntryLen = int(u)
	}
	_ = preEntryLen

	entryFlag, err := r.ReadByte()
	if err != nil {
		return "", err
	}

	// String: flag < zipStrMask
	encoding := entryFlag
	if encoding < zipStrMask {
		encoding &= zipStrMask
	}
	switch encoding {
	case zipStr06B:
		return r.ReadFixedString(int(entryFlag & 0x3F))
	case zipStr14B:
		b2, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		l := binary.BigEndian.Uint16([]byte{entryFlag & 0x3F, b2})
		return r.ReadFixedString(int(l))
	case zipStr32B:
		bUint32, err := r.GetBUint32()
		if err != nil {
			return "", err
		}
		return r.ReadFixedString(int(bUint32))
	case zipInt8B:
		uInt8, err := r.GetUint8()
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(int64(uInt8), 10), nil
	case zipInt16B:
		uInt16, err := r.GetLUint16()
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(int64(uInt16), 10), nil
	case zipInt24B:
		uInt24, err := r.GetLUint24()
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(int64(int32(uInt24)), 10), nil
	case zipInt32B:
		uInt32, err := r.GetLUint32()
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(int64(uInt32), 10), nil
	case zipInt64B:
		uInt64, err := r.GetLUint64()
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(int64(uInt64), 10), nil
	default:
		if encoding >= zipIntImmMin && encoding <= zipIntImmMax {
			v := (encoding & zipIntImmMask) - 1
			return strconv.FormatInt(int64(v), 10), nil
		}
		return "", fmt.Errorf("bad encoding")
	}
}
