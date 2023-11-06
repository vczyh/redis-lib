package rdb

import "fmt"

type Module struct{}

func parseModule(r *Reader, valueType byte) (*Module, error) {
	// TODO
	moduleId, err := r.GetLengthUInt64()
	if err != nil {
		return nil, err
	}
	_ = moduleId

	return nil, fmt.Errorf("unsupport module type")
}
