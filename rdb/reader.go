package rdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
)

type rdbReader struct {
	r io.Reader
}

func newRdbReader(r io.Reader) *rdbReader {
	return &rdbReader{r: r}
}

func (r *rdbReader) GetInt8() (int8, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	return int8(b), nil
}

func (r *rdbReader) GetUint8() (uint8, error) {
	return r.ReadByte()
}

func (r *rdbReader) GetLUint16() (uint16, error) {
	b, err := r.ReadFixedBytes(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b), nil
}

func (r *rdbReader) GetLUint24() (uint32, error) {
	b, err := r.ReadFixedBytes(3)
	if err != nil {
		return 0, err
	}
	bs := append([]byte{0}, b...)
	return binary.LittleEndian.Uint32(bs) >> 8, nil
}

func (r *rdbReader) GetLUint32() (uint32, error) {
	b, err := r.ReadFixedBytes(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func (r *rdbReader) GetBUint32() (uint32, error) {
	b, err := r.ReadFixedBytes(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (r *rdbReader) GetLUint64() (uint64, error) {
	b, err := r.ReadFixedBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (r *rdbReader) GetBUint64() (uint64, error) {
	b, err := r.ReadFixedBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func (r *rdbReader) GetLengthString() (string, error) {
	encoding, n, err := r.GetEncodingLength()
	if err != nil {
		return "", err
	}
	switch encoding {
	case lengthEncodingLength:
		return r.ReadFixedString(int(n))
	case lengthEncodingInteger:
		return strconv.Itoa(int(n)), nil
	default:
		return "", fmt.Errorf("unsupported encoding %d for GetLengthString", encoding)
	}
}

func (r *rdbReader) GetLengthBytes() ([]byte, error) {
	encoding, n, err := r.GetEncodingLength()
	if err != nil {
		return nil, err
	}
	switch encoding {
	case lengthEncodingLength:
		return r.ReadFixedBytes(int(n))
	default:
		return nil, fmt.Errorf("unsupported encoding %d for GetLengthString", encoding)
	}
}

func (r *rdbReader) GetLengthInt() (int, error) {
	encoding, n, err := r.GetEncodingLength()
	if err != nil {
		return 0, err
	}
	switch encoding {
	case lengthEncodingLength, lengthEncodingInteger:
		return int(n), nil
	default:
		return 0, fmt.Errorf("unsupported encoding %d for GetLengthInt", encoding)
	}
}

func (r *rdbReader) GetLengthUInt64() (uint64, error) {
	encoding, n, err := r.GetEncodingLength()
	if err != nil {
		return 0, err
	}
	switch encoding {
	case lengthEncodingLength, lengthEncodingInteger:
		return n, nil
	default:
		return 0, fmt.Errorf("unsupported encoding %d for GetLengthInt", encoding)
	}
}

func (r *rdbReader) ReadByte() (byte, error) {
	bs, err := r.ReadFixedBytes(1)
	if err != nil {
		return 0, err
	}
	return bs[0], nil
}

func (r *rdbReader) Read(p []byte) (n int, err error) {
	return r.r.Read(p)
}

func (r *rdbReader) ReadFixedBytes(size int) ([]byte, error) {
	bs := make([]byte, size)
	_, err := io.ReadFull(r.r, bs)
	return bs, err
}

func (r *rdbReader) ReadFixedString(size int) (string, error) {
	bs, err := r.ReadFixedBytes(size)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

type lengthEncoding uint8

const (
	lengthEncodingLength lengthEncoding = iota
	lengthEncodingInteger
	lengthEncodingCompressed
)

func (r *rdbReader) GetEncodingLength() (lengthEncoding, uint64, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	switch b >> 6 {
	case 0:
		return lengthEncodingLength, uint64(b & 0x3F), nil
	case 1:
		b2, err := r.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		size := binary.BigEndian.Uint16([]byte{b & 0x3F, b2})
		return lengthEncodingLength, uint64(size), nil
	case 2:
		b2, err := r.ReadFixedBytes(4)
		if err != nil {
			return 0, 0, err
		}
		//bs := []byte{b & 0x3F}
		//bs = append(bs, b2...)
		size := binary.BigEndian.Uint32(b2)
		return lengthEncodingLength, uint64(size), nil
	case 3:
		switch b & 0x3F {
		case 0:
			b2, err := r.ReadByte()
			if err != nil {
				return 0, 0, err
			}
			return lengthEncodingInteger, uint64(b2), nil
		case 1:
			b2, err := r.ReadFixedBytes(2)
			if err != nil {
				return 0, 0, err
			}
			v := binary.LittleEndian.Uint16(b2)
			return lengthEncodingInteger, uint64(v), nil
		case 2:
			b2, err := r.ReadFixedBytes(4)
			if err != nil {
				return 0, 0, err
			}
			v := binary.LittleEndian.Uint32(b2)
			return lengthEncodingInteger, uint64(v), nil
		case 3:
			// TODO comparessed
			panic("compressed")
		default:
			return 0, 0, fmt.Errorf("unsupported 6 bits: %x", b&0x3F)
		}
	default:
		return 0, 0, fmt.Errorf("unsupported bit prefix: %x", b)
	}
}
