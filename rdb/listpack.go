package rdb

import (
	"fmt"
	"math"
	"strconv"
)

const (
	LPEncoding7BitUint     = 0
	LPEncoding7BitUintMask = 0x80

	LPEncoding6BitStr     = 0x80
	LPEncoding6BitStrMask = 0xC0

	LPEncoding13BitInt     = 0xC0
	LPEncoding13BitIntMask = 0xE0

	LPEncoding12BitStr     = 0xE0
	LPEncoding12BitStrMask = 0xF0

	LPEncoding16BitInt     = 0xF1
	LPEncoding16BitIntMask = 0xFF

	LPEncoding24BitInt     = 0xF2
	LPEncoding24BitIntMask = 0xFF

	LPEncoding32BitInt     = 0xF3
	LPEncoding32BitIntMask = 0xFF

	LPEncoding64BitInt     = 0xF4
	LPEncoding64BitIntMask = 0xFF

	LPEncoding32BitStr     = 0xF0
	LPEncoding32BitStrMask = 0xFF
)

func parseListPack(r *Reader) ([]string, error) {
	// Total length
	_, err := r.GetLUint32()
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

func parseListPackEntry(r *Reader) (string, error) {
	encoding, err := r.ReadByte()
	if err != nil {
		return "", err
	}

	var val int64
	var uVal, negStart, negMax uint64

	switch {
	case encoding&LPEncoding7BitUintMask == LPEncoding7BitUint:
		negStart = math.MaxUint64
		negMax = 0
		uVal = uint64(encoding & 0x7f)
		// backLen bytes
		if _, err := r.ReadFixedBytes(1); err != nil {
			return "", err
		}
	case encoding&LPEncoding6BitStrMask == LPEncoding6BitStr:
		count := int(encoding & 0x3f)
		s, err := r.ReadFixedString(count)
		if err != nil {
			return "", err
		}
		if _, err := r.ReadFixedBytes(lpEncodeBackLen(1 + count)); err != nil {
			return "", err
		}
		return s, nil
	case encoding&LPEncoding13BitIntMask == LPEncoding13BitInt:
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
	case encoding&LPEncoding16BitIntMask == LPEncoding16BitInt:
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
	case encoding&LPEncoding24BitIntMask == LPEncoding24BitInt:
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
	case encoding&LPEncoding32BitIntMask == LPEncoding32BitInt:
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
	case encoding&LPEncoding64BitIntMask == LPEncoding64BitInt:
		uVal, err = r.GetLUint64()
		if err != nil {
			return "", err
		}
		negStart = uint64(1) << 63
		negMax = math.MaxUint64
		if _, err := r.ReadFixedBytes(1); err != nil {
			return "", err
		}
	case encoding&LPEncoding12BitStrMask == LPEncoding12BitStr:
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
	case encoding&LPEncoding32BitStrMask == LPEncoding32BitStr:
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
