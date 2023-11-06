package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// https://redis.io/docs/reference/protocol-spec/
const (
	DataTypeSimpleString   = '+' // RESP2
	DataTypeSimpleError    = '-' // RESP2
	DataTypeInteger        = ':' // RESP2
	DataTypeBulkString     = '$' // RESP2
	DataTypeArray          = '*' // RESP2
	DataTypeNull           = '_' // RESP3
	DataTypeBoolean        = '#' // RESP3
	DataTypeDouble         = ',' // RESP3
	DataTypeBigNumbers     = '(' // RESP3
	DataTypeBulkError      = '!' // RESP3
	DataTypeVerbatimString = '=' // RESP3
	DataTypeMap            = '%' // RESP3
	DataTypeSet            = '~' // RESP3
	DataTypePush           = '>' // RESP3
)

var (
	Separator = []byte{'\r', '\n'}
)

func ReadString(r *bufio.Reader) (string, error) {
	data, err := ReadData(r)
	if err != nil {
		return "", err
	}

	switch data[0] {
	case DataTypeNull:
		return "", nil
	case DataTypeSimpleString:
		return string(data[1:]), nil
	case DataTypeBulkString:
		size, err := getLen(data)
		if err != nil {
			return "", err
		}
		v := make([]byte, size+2)
		if _, err := io.ReadFull(r, v); err != nil {
			return "", err
		}
		return string(v[:size]), nil
	}

	return "", fmt.Errorf("not string type")
}

func ReadSeparatedBytes(r *bufio.Reader) ([]byte, error) {
	data, err := r.ReadBytes('\r')
	if err != nil {
		return nil, err
	}
	if _, err := r.Discard(1); err != nil {
		return nil, err
	}
	return data[:len(data)-1], nil
}

func ReadData(r *bufio.Reader) ([]byte, error) {
	line, err := ReadSeparatedBytes(r)
	if err != nil {
		return nil, err
	}

	switch line[0] {
	case DataTypeSimpleError:
		return nil, errors.New(string(line[1:]))
	case DataTypeBulkError:
		line, err := ReadSeparatedBytes(r)
		l, err := getLen(line)
		if err != nil {
			return nil, err
		}
		data := make([]byte, l+2)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}
		return nil, errors.New(string(data[:l]))
	}

	return line, nil
}

func getLen(data []byte) (int, error) {
	return strconv.Atoi(string(data[1:]))
}
