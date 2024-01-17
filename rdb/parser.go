package rdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
)

const (
	opCodeFunction2 = 245
	opcodeFunction  = 246
	opCodeModuleAux = 247
	opCodeIdle      = 248
	opCodeFreq      = 249
	opCodeAux       = 250
	opCodeResizeDb  = 0xFB
	opExpireTimeMs  = 0xFC
	opExpireTime    = 0xFD
	opCodeSelectDb  = 0xFE
	opCodeEOF       = 0xFF

	rdbTypeString           = 0
	rdbTypeList             = 1
	rdbTypeSet              = 2
	rdbTypeZSet             = 3
	rdbTypeHash             = 4
	rdbTypeZSet2            = 5 // ZSET version 2 with doubles stored in binary.
	rdbTypeModulePreGA      = 6
	rdbTypeModule2          = 7 // Module value with annotations for parsing without the generating module being loaded.
	rdbTypeHashZipMap       = 9
	rdbTypeZipList          = 10
	rdbTypeIntSet           = 11
	rdbTypeZSetZipList      = 12
	rdbTypeHashZipList      = 13
	rdbTypeListQuickList    = 14
	rdbTypeStreamListPacks  = 15
	rdbTypeHashListPack     = 16
	rdbTypeZSetListPack     = 17
	rdbTypeListQuickList2   = 18
	rdbTypeStreamListPacks2 = 19
	rdbTypeSetListPack      = 20
	rdbTypeStreamListPacks3 = 21
)

type Parser struct {
	file string
	fd   *os.File
	r    *rdbReader

	version int
}

func NewParser(name string) (*Parser, error) {
	p := &Parser{
		file: name,
	}
	return p, nil
}

func NewReaderParser(r io.Reader) (*Parser, error) {
	p := &Parser{
		r: newRdbReader(r),
	}
	return p, nil
}

func (p *Parser) Parse() (*EventStreamer, error) {
	if p.file != "" {
		f, err := os.Open(p.file)
		if err != nil {
			return nil, err
		}
		p.fd = f
		r := newRdbReader(f)
		p.r = r
	}

	eventC := make(chan *eventWrapper)
	es := newEventStreamer(eventC)

	go func() {
		defer func() {
			close(eventC)
			p.close()
		}()

		if err := p.parse(eventC); err != nil {
			eventC <- &eventWrapper{e: nil, err: err}
		}
	}()

	return es, nil
}

func (p *Parser) close() {
	if p.fd != nil {
		p.fd.Close()
	}
}

func (p *Parser) parse(eventC chan *eventWrapper) error {
	// magic number
	magic, err := p.r.ReadFixedBytes(5)
	if err != nil {
		return err
	}
	magicEvent := &MagicNumberEvent{MagicNumber: magic}
	eventC <- &eventWrapper{
		e: &RedisRdbEvent{
			EventType: EventTypeMagicNumber,
			Event:     magicEvent,
		},
	}

	// version
	version, err := p.r.ReadFixedBytes(4)
	if err != nil {
		return err
	}
	versionNumber, err := strconv.Atoi(string(version))
	if err != nil {
		return err
	}
	p.version = versionNumber
	versionEvent := &VersionEvent{Version: versionNumber}
	eventC <- &eventWrapper{
		e: &RedisRdbEvent{
			EventType: EventTypeVersion,
			Event:     versionEvent,
		},
	}

	var expireTime uint64
	var isEnd bool

	for !isEnd {
		b, err := p.r.ReadByte()
		if err != nil {
			return err
		}
		switch b {
		case opCodeAux:
			e, err := p.parseAuxiliaryFields()
			if err != nil {
				return err
			}
			eventC <- &eventWrapper{
				e: &RedisRdbEvent{
					EventType: opCodeAux,
					Event:     e,
				},
			}
			continue
		case opCodeSelectDb:
			e, err := p.parseSelectDb()
			if err != nil {
				return err
			}
			eventC <- &eventWrapper{
				e: &RedisRdbEvent{
					EventType: EventTypeSelectDb,
					Event:     e,
				},
			}
			continue
		case opCodeResizeDb:
			e, err := p.parseResizeDb()
			if err != nil {
				return err
			}
			eventC <- &eventWrapper{
				e: &RedisRdbEvent{
					EventType: EventTypeResizeDb,
					Event:     e,
				},
			}
			continue
		case opExpireTime:
			expireTime, err = p.parseExpireTime()
			if err != nil {
				return err
			}
			continue
		case opExpireTimeMs:
			expireTime, err = p.parseExpireTimeMs()
			if err != nil {
				return err
			}
			continue
		case opCodeEOF:
			isEnd = true
			continue
		}

		e, err := p.parseEntryWithValueType(b, expireTime)
		if err != nil {
			return err
		}
		eventC <- &eventWrapper{e: e}
	}

	if p.version >= 5 {
		// TODO compare?
		if _, err := p.r.ReadFixedBytes(8); err != nil {
			return err
		}
	}

	return nil
}

func (p *Parser) parseAuxiliaryFields() (*AuxFieldEvent, error) {
	field, err := p.r.GetLengthString()
	if err != nil {
		return nil, err
	}
	value, err := p.r.GetLengthString()
	if err != nil {
		return nil, err
	}
	return &AuxFieldEvent{
		Filed: field,
		Value: value,
	}, nil
}

func (p *Parser) parseSelectDb() (*SelectDbEvent, error) {
	// DB number
	dbNumber, err := p.r.GetLengthInt()
	if err != nil {
		return nil, err
	}
	//fmt.Printf("Db number: %d\n", dbNumber)
	return &SelectDbEvent{Db: dbNumber}, nil
}

func (p *Parser) parseResizeDb() (*ResizeDbEvent, error) {
	dbSize, err := p.r.GetLengthInt()
	if err != nil {
		return nil, err
	}
	dbExpiresSize, err := p.r.GetLengthInt()
	if err != nil {
		return nil, err
	}
	return &ResizeDbEvent{
		dbSize:       dbSize,
		dbExpireSize: dbExpiresSize,
	}, nil
}

func (p *Parser) parseExpireTime() (uint64, error) {
	b, err := p.r.ReadFixedBytes(4)
	if err != nil {
		return 0, nil
	}
	expireAt := binary.LittleEndian.Uint32(b)
	return uint64(expireAt) * 1000, nil
}

func (p *Parser) parseExpireTimeMs() (uint64, error) {
	b, err := p.r.ReadFixedBytes(8)
	if err != nil {
		return 0, nil
	}
	expireAt := binary.LittleEndian.Uint64(b)
	return expireAt, nil
}

func (p *Parser) parseEntryWithValueType(valueType byte, expireAt uint64) (*RedisRdbEvent, error) {
	// TODO expires?

	key, err := p.r.GetLengthString()
	if err != nil {
		return nil, err
	}

	switch valueType {
	case rdbTypeString:
		event, err := parseString(key, p.r)
		if err != nil {
			return nil, err
		}
		return &RedisRdbEvent{EventType: EventTypeStringObject, Event: event}, nil
	case rdbTypeList, rdbTypeZipList, rdbTypeListQuickList, rdbTypeListQuickList2:
		event, err := parseList(key, p.r, valueType)
		if err != nil {
			return nil, err
		}
		return &RedisRdbEvent{EventType: EventTypeListObject, Event: event}, nil
	case rdbTypeSet, rdbTypeSetListPack, rdbTypeIntSet:
		event, err := parseSet(key, p.r, valueType)
		if err != nil {
			return nil, err
		}
		return &RedisRdbEvent{EventType: EventTypeSetObject, Event: event}, nil
	case rdbTypeZSetZipList, rdbTypeZSetListPack, rdbTypeZSet, rdbTypeZSet2:
		event, err := parseZSet(key, p.r, valueType)
		if err != nil {
			return nil, err
		}
		return &RedisRdbEvent{EventType: EventTypeZSetObject, Event: event}, nil
	case rdbTypeHashZipList, rdbTypeHashListPack, rdbTypeHash:
		event, err := parseHash(key, p.r, valueType)
		if err != nil {
			return nil, err
		}
		return &RedisRdbEvent{EventType: EventTypeHashObject, Event: event}, nil
	case rdbTypeStreamListPacks, rdbTypeStreamListPacks2:
		event, err := parseStream(key, p.r, valueType)
		if err != nil {
			return nil, err
		}
		return &RedisRdbEvent{EventType: EventTypeStreamObject, Event: event}, nil
	default:
		return nil, fmt.Errorf("unsupported rdb value type: 0x%x", valueType)
	}
}
