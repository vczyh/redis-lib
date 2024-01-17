package rdb

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
)

const (
	lpEncoding7BitUint     = 0
	lpEncoding7BitUintMask = 0x80

	lpEncoding6BitStr     = 0x80
	lpEncoding6BitStrMask = 0xC0

	lpEncoding13BitInt     = 0xC0
	lpEncoding13BitIntMask = 0xE0

	lpEncoding12BitStr     = 0xE0
	lpEncoding12BitStrMask = 0xF0

	lpEncoding16BitInt     = 0xF1
	lpEncoding16BitIntMask = 0xFF

	lpEncoding24BitInt     = 0xF2
	lpEncoding24BitIntMask = 0xFF

	lpEncoding32BitInt     = 0xF3
	lpEncoding32BitIntMask = 0xFF

	lpEncoding64BitInt     = 0xF4
	lpEncoding64BitIntMask = 0xFF

	lpEncoding32BitStr     = 0xF0
	lpEncoding32BitStrMask = 0xFF
)

func parseListPack(r *rdbReader) ([]string, error) {
	listPackBytes, err := r.GetLengthBytes()
	if err != nil {
		return nil, err
	}
	r = newRdbReader(bytes.NewReader(listPackBytes))

	// Total length
	_, err = r.GetLUint32()
	if err != nil {
		return nil, err
	}
	// Element size
	size, err := r.GetLUint16()
	if err != nil {
		return nil, err
	}
	members := make([]string, size)
	for i := 0; i < int(size); i++ {
		entry, err := parseListPackEntry(r)
		if err != nil {
			return nil, err
		}
		members[i] = entry
	}

	eof, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if eof != 0xff {
		return nil, fmt.Errorf("listpack must end of 0xff: %x", eof)
	}
	return members, nil
}

func parseListPackEntry(r *rdbReader) (string, error) {
	encoding, err := r.ReadByte()
	if err != nil {
		return "", err
	}

	var val int64
	var uVal, negStart, negMax uint64

	switch {
	case encoding&lpEncoding7BitUintMask == lpEncoding7BitUint:
		negStart = math.MaxUint64
		negMax = 0
		uVal = uint64(encoding & 0x7f)
		// backLen bytes
		if _, err := r.ReadFixedBytes(1); err != nil {
			return "", err
		}
	case encoding&lpEncoding6BitStrMask == lpEncoding6BitStr:
		count := int(encoding & 0x3f)
		s, err := r.ReadFixedString(count)
		if err != nil {
			return "", err
		}
		if _, err := r.ReadFixedBytes(lpEncodeBackLen(1 + count)); err != nil {
			return "", err
		}
		return s, nil
	case encoding&lpEncoding13BitIntMask == lpEncoding13BitInt:
		b2, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		uVal = (uint64(encoding&0x1f) << 8) | uint64(b2)
		negStart = uint64(1) << 12
		negMax = 8191
		if _, err := r.ReadFixedBytes(1); err != nil {
			return "", err
		}
	case encoding&lpEncoding16BitIntMask == lpEncoding16BitInt:
		lUint16, err := r.GetLUint16()
		if err != nil {
			return "", err
		}
		uVal = uint64(lUint16)
		negStart = uint64(1) << 15
		negMax = math.MaxUint16
		if _, err := r.ReadFixedBytes(1); err != nil {
			return "", err
		}
	case encoding&lpEncoding24BitIntMask == lpEncoding24BitInt:
		lUint24, err := r.GetLUint24()
		if err != nil {
			return "", err
		}
		uVal = uint64(lUint24)
		negStart = uint64(1) << 23
		negMax = math.MaxUint32 >> 8
		if _, err := r.ReadFixedBytes(1); err != nil {
			return "", err
		}
	case encoding&lpEncoding32BitIntMask == lpEncoding32BitInt:
		lUint32, err := r.GetLUint32()
		if err != nil {
			return "", err
		}
		uVal = uint64(lUint32)
		negStart = uint64(1) << 31
		negMax = math.MaxUint32
		if _, err := r.ReadFixedBytes(1); err != nil {
			return "", err
		}
	case encoding&lpEncoding64BitIntMask == lpEncoding64BitInt:
		uVal, err = r.GetLUint64()
		if err != nil {
			return "", err
		}
		negStart = uint64(1) << 63
		negMax = math.MaxUint64
		if _, err := r.ReadFixedBytes(1); err != nil {
			return "", err
		}
	case encoding&lpEncoding12BitStrMask == lpEncoding12BitStr:
		b2, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		count := int(encoding&0xf)<<8 | int(b2)
		s, err := r.ReadFixedString(count)
		if err != nil {
			return "", err
		}
		if _, err := r.ReadFixedBytes(lpEncodeBackLen(2 + count)); err != nil {
			return "", err
		}
		return s, nil
	case encoding&lpEncoding32BitStrMask == lpEncoding32BitStr:
		count, err := r.GetLUint32()
		if err != nil {
			return "", err
		}
		s, err := r.ReadFixedString(int(count))
		if err != nil {
			return "", err
		}
		if _, err := r.ReadFixedBytes(lpEncodeBackLen(5 + int(count))); err != nil {
			return "", err
		}
		return s, nil
	default:
		uVal = 12345678900000000 + uint64(encoding)
		negStart = math.MaxUint64
		negMax = 0
	}

	/* We reach this code path only for integer encodings.
	 * Convert the unsigned value to the signed one using two's complement
	 * rule. */
	if uVal >= negStart {
		uVal = negMax - uVal
		val = int64(uVal)
		val = -val - 1
	} else {
		val = int64(uVal)
	}

	return strconv.FormatInt(val, 10), nil
}

// listpack.c::lpEncodeBacklen
func lpEncodeBackLen(entryLen int) int {
	switch {
	case entryLen <= 127:
		return 1
	case entryLen < 16383:
		return 2
	case entryLen < 2097151:
		return 3
	case entryLen < 268435455:
		return 4
	default:
		return 5
	}
}
