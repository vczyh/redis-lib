package rdb

import "fmt"

type ModuleObjectEvent struct{}

func parseModule(r *rdbReader, valueType byte) (*ModuleObjectEvent, error) {
	// TODO
	moduleId, err := r.GetLengthUInt64()
	if err != nil {
		return nil, err
	}
	_ = moduleId

	return nil, fmt.Errorf("unsupport module type")
}
