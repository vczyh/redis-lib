package rdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
)

type Reader struct {
	r io.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

func (r *Reader) GetInt8() (int8, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	return int8(b), nil
}

func (r *Reader) GetUint8() (uint8, error) {
	return r.ReadByte()
}

func (r *Reader) GetLUint16() (uint16, error) {
	b, err := r.ReadFixedBytes(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b), nil
}

func (r *Reader) GetLUint24() (uint32, error) {
	b, err := r.ReadFixedBytes(3)
	if err != nil {
		return 0, err
	}
	bs := append([]byte{0}, b...)
	return binary.LittleEndian.Uint32(bs) >> 8, nil
}

func (r *Reader) GetLUint32() (uint32, error) {
	b, err := r.ReadFixedBytes(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func (r *Reader) GetBUint32() (uint32, error) {
	b, err := r.ReadFixedBytes(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (r *Reader) GetLUint64() (uint64, error) {
	b, err := r.ReadFixedBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (r *Reader) GetBUint64() (uint64, error) {
	b, err := r.ReadFixedBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func (r *Reader) GetLengthString() (string, error) {
	encoding, n, err := r.GetEncodingLength()
	if err != nil {
		return "", err
	}
	switch encoding {
	case LengthEncodingLength:
		return r.ReadFixedString(int(n))
	case LengthEncodingInteger:
		return strconv.Itoa(int(n)), nil
	default:
		return "", fmt.Errorf("unsupported encoding %d for GetLengthString", encoding)
	}
}

func (r *Reader) GetLengthBytes() ([]byte, error) {
	encoding, n, err := r.GetEncodingLength()
	if err != nil {
		return nil, err
	}
	switch encoding {
	case LengthEncodingLength:
		return r.ReadFixedBytes(int(n))
	default:
		return nil, fmt.Errorf("unsupported encoding %d for GetLengthString", encoding)
	}
}

func (r *Reader) GetLengthInt() (int, error) {
	encoding, n, err := r.GetEncodingLength()
	if err != nil {
		return 0, err
	}
	switch encoding {
	case LengthEncodingLength, LengthEncodingInteger:
		return int(n), nil
	default:
		return 0, fmt.Errorf("unsupported encoding %d for GetLengthInt", encoding)
	}
}

func (r *Reader) GetLengthUInt64() (uint64, error) {
	encoding, n, err := r.GetEncodingLength()
	if err != nil {
		return 0, err
	}
	switch encoding {
	case LengthEncodingLength, LengthEncodingInteger:
		return n, nil
	default:
		return 0, fmt.Errorf("unsupported encoding %d for GetLengthInt", encoding)
	}
}

func (r *Reader) ReadByte() (byte, error) {
	bs, err := r.ReadFixedBytes(1)
	if err != nil {
		return 0, err
	}
	return bs[0], nil
}

func (r *Reader) Read(p []byte) (n int, err error) {
	return r.r.Read(p)
}

func (r *Reader) ReadFixedBytes(size int) ([]byte, error) {
	bs := make([]byte, size)
	_, err := io.ReadFull(r.r, bs)
	return bs, err
}

func (r *Reader) ReadFixedString(size int) (string, error) {
	bs, err := r.ReadFixedBytes(size)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

type LengthEncoding uint8

const (
	LengthEncodingLength LengthEncoding = iota
	LengthEncodingInteger
	LengthEncodingCompressed
)

func (r *Reader) GetEncodingLength() (LengthEncoding, uint64, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	switch b >> 6 {
	case 0:
		return LengthEncodingLength, uint64(b & 0x3F), nil
	case 1:
		b2, err := r.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		size := binary.BigEndian.Uint16([]byte{b & 0x3F, b2})
		return LengthEncodingLength, uint64(size), nil
	case 2:
		b2, err := r.ReadFixedBytes(4)
		if err != nil {
			return 0, 0, err
		}
		//bs := []byte{b & 0x3F}
		//bs = append(bs, b2...)
		size := binary.BigEndian.Uint32(b2)
		return LengthEncodingLength, uint64(size), nil
	case 3:
		switch b & 0x3F {
		case 0:
			b2, err := r.ReadByte()
			if err != nil {
				return 0, 0, err
			}
			return LengthEncodingInteger, uint64(b2), nil
		case 1:
			b2, err := r.ReadFixedBytes(2)
			if err != nil {
				return 0, 0, err
			}
			v := binary.LittleEndian.Uint16(b2)
			return LengthEncodingInteger, uint64(v), nil
		case 2:
			b2, err := r.ReadFixedBytes(4)
			if err != nil {
				return 0, 0, err
			}
			v := binary.LittleEndian.Uint32(b2)
			return LengthEncodingInteger, uint64(v), nil
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
