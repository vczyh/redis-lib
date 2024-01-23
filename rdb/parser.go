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
	opCodeResizeDb  = 251
	opExpireTimeMs  = 252
	opExpireTime    = 253
	opCodeSelectDb  = 254
	opCodeEOF       = 255

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

	dbId := 0
	var expireTime int64 = -1

	var isEnd bool
	for !isEnd {
		rdbType, err := p.r.ReadByte()
		if err != nil {
			return err
		}
		switch rdbType {
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
		case opCodeFreq:
			// LFU frequency.
			if err := p.parseFreq(); err != nil {
				return err
			}
			continue
		case opCodeIdle:
			// LRU idle time.
			_, err := p.parseIdle()
			if err != nil {
				return err
			}
			continue
		case opCodeEOF:
			isEnd = true
			continue
		case opCodeSelectDb:
			e, err := p.parseSelectDb()
			if err != nil {
				return err
			}
			dbId = e.Db
			eventC <- &eventWrapper{
				e: &RedisRdbEvent{
					EventType: EventTypeSelectDb,
					Event:     e,
				},
			}
			continue
		case opCodeResizeDb:
			// RESIZEDB: Hint about the size of the keys in the currently
			// selected data base, in order to avoid useless rehashing.
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
		case opCodeAux:
			// AUX: generic string-string fields. Use to add state to RDB
			// which is backward compatible. Implementations of RDB loading
			// are required to skip AUX fields they don't understand.
			//
			// An AUX field is composed of two strings: key and value.
			e, err := p.parseAuxiliaryFields()
			if err != nil {
				return err
			}
			eventC <- &eventWrapper{
				e: &RedisRdbEvent{
					EventType: EventTypeAuxField,
					Event:     e,
				},
			}
			continue
		case opCodeModuleAux:
			// Load module data that is not related to the Redis key space.
			// Such data can be potentially be stored both before and after the
			// RDB keys-values section.
			// TODO
			continue
		case opcodeFunction:
			return fmt.Errorf("pre-release function format not supported")
		case opCodeFunction2:
			// TODO
			return fmt.Errorf("function not suuported")
		}

		// Load object key.
		key, err := p.parseKey()
		if err != nil {
			return err
		}
		// Load object value.
		e, err := p.parseEntryWithValueType(rdbType, key, dbId, expireTime)
		if err != nil {
			return err
		}
		eventC <- &eventWrapper{e: e}

		// Reset state.
		expireTime = -1
	}

	if p.version >= 5 {
		// TODO compare?
		if _, err := p.r.ReadFixedBytes(8); err != nil {
			return err
		}
	}

	return nil
}

func (p *Parser) parseFreq() error {
	_, err := p.r.ReadByte()
	return err
}

func (p *Parser) parseIdle() (uint64, error) {
	return p.r.GetLengthUInt64()
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

func (p *Parser) parseExpireTime() (int64, error) {
	b, err := p.r.ReadFixedBytes(4)
	if err != nil {
		return 0, nil
	}
	expireAt := binary.LittleEndian.Uint32(b)
	return int64(expireAt) * 1000, nil
}

func (p *Parser) parseExpireTimeMs() (int64, error) {
	b, err := p.r.ReadFixedBytes(8)
	if err != nil {
		return 0, nil
	}
	expireAt := binary.LittleEndian.Uint64(b)
	return int64(expireAt), nil
}

func (p *Parser) parseKey() (string, error) {
	return p.r.GetLengthString()
}

func (p *Parser) parseEntryWithValueType(valueType byte, key string, DbId int, expireAt int64) (*RedisRdbEvent, error) {
	redisKey := RedisKey{
		DbId:     DbId,
		Key:      key,
		expireAt: expireAt,
	}

	switch valueType {
	case rdbTypeString:
		event, err := parseString(redisKey, p.r)
		if err != nil {
			return nil, err
		}
		return &RedisRdbEvent{EventType: EventTypeStringObject, Event: event}, nil
	case rdbTypeList, rdbTypeZipList, rdbTypeListQuickList, rdbTypeListQuickList2:
		event, err := parseList(redisKey, p.r, valueType)
		if err != nil {
			return nil, err
		}
		return &RedisRdbEvent{EventType: EventTypeListObject, Event: event}, nil
	case rdbTypeSet, rdbTypeSetListPack, rdbTypeIntSet:
		event, err := parseSet(redisKey, p.r, valueType)
		if err != nil {
			return nil, err
		}
		return &RedisRdbEvent{EventType: EventTypeSetObject, Event: event}, nil
	case rdbTypeZSetZipList, rdbTypeZSetListPack, rdbTypeZSet, rdbTypeZSet2:
		event, err := parseZSet(redisKey, p.r, valueType)
		if err != nil {
			return nil, err
		}
		return &RedisRdbEvent{EventType: EventTypeZSetObject, Event: event}, nil
	case rdbTypeHashZipList, rdbTypeHashListPack, rdbTypeHash:
		event, err := parseHash(redisKey, p.r, valueType)
		if err != nil {
			return nil, err
		}
		return &RedisRdbEvent{EventType: EventTypeHashObject, Event: event}, nil
	case rdbTypeStreamListPacks, rdbTypeStreamListPacks2:
		event, err := parseStream(redisKey, p.r, valueType)
		if err != nil {
			return nil, err
		}
		return &RedisRdbEvent{EventType: EventTypeStreamObject, Event: event}, nil
	default:
		return nil, fmt.Errorf("unsupported rdb value type: 0x%x", valueType)
	}
}
