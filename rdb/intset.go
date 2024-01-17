package rdb

import "bytes"

func parseIntSet(r *rdbReader) ([]int64, error) {
	b, err := r.GetLengthBytes()
	if err != nil {
		return nil, err
	}
	r = newRdbReader(bytes.NewReader(b))

	encoding, err := r.GetLUint32()
	if err != nil {
		return nil, err
	}
	length, err := r.GetLUint32()
	if err != nil {
		return nil, err
	}

	members := make([]int64, length)
	for i := 0; i < int(length); i++ {
		switch encoding {
		case 2:
			v, err := r.GetLUint16()
			if err != nil {
				return nil, err
			}
			members[i] = int64(v)
		case 4:
			v, err := r.GetLUint32()
			if err != nil {
				return nil, err
			}
			members[i] = int64(v)
		case 8:
			v, err := r.GetLUint16()
			if err != nil {
				return nil, err
			}
			members[i] = int64(v)
		}
	}

	return members, nil
}
