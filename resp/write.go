package resp

import (
	"io"
	"strconv"
)

func WriteBulkString(w io.Writer, str string) error {
	data := []byte{
		DataTypeBulkString,
	}
	data = append(data, strconv.Itoa(len(str))...)
	data = append(data, Separator...)
	data = append(data, str...)
	data = append(data, Separator...)
	if _, err := w.Write(data); err != nil {
		return err
	}
	return nil
}

func WriteArray(w io.Writer, args ...string) error {
	data := []byte{
		DataTypeArray,
	}
	data = append(data, strconv.Itoa(len(args))...)
	data = append(data, Separator...)
	if _, err := w.Write(data); err != nil {
		return err
	}

	for _, arg := range args {
		if err := WriteBulkString(w, arg); err != nil {
			return err
		}
	}
	return nil
}
