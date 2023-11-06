package rdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
)

const (
	OpCodeFunction2 = 245
	OpcodeFunction  = 246
	OpCodeModuleAux = 247
	OpCodeIdle      = 248
	OpCodeFreq      = 249
	OpCodeAux       = 250
	OpCodeResizeDb  = 0xFB
	OpExpireTimeMs  = 0xFC
	OpExpireTime    = 0xFD
	OpCodeSelectDb  = 0xFE
	OpCodeEOF       = 0xFF

	ValueTypeString           = 0
	ValueTypeList             = 1
	ValueTypeSet              = 2
	ValueTypeZSet             = 3
	ValueTypeHash             = 4
	ValueTypeZSet2            = 5 // ZSET version 2 with doubles stored in binary.
	ValueTypeModule           = 6
	ValueTypeModule2          = 7 // Module value with annotations for parsing without the generating module being loaded.
	ValueTypeHashZipMap       = 9
	ValueTypeZipList          = 10
	ValueTypeIntSet           = 11
	ValueTypeZSetZipList      = 12
	ValueTypeHashZipList      = 13
	ValueTypeListQuickList    = 14
	ValueTypeStreamListPacks  = 15
	ValueTypeHashListPack     = 16
	ValueTypeZSetListPack     = 17
	ValueTypeListQuickList2   = 18
	ValueTypeStreamListPacks2 = 19
)

type Parser struct {
	file string
	fd   *os.File
	r    *Reader

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
		r: NewReader(r),
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
		r := NewReader(f)
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
			eventC <- &eventWrapper{
				e:   nil,
				err: err,
			}
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
	eventC <- &eventWrapper{
		e:   &MagicNumberEvent{MagicNumber: magic},
		err: nil,
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
	eventC <- &eventWrapper{
		e:   &VersionEvent{Version: versionNumber},
		err: nil,
	}

	var expireTime uint64
	var isEnd bool

	for !isEnd {
		b, err := p.r.ReadByte()
		if err != nil {
			return err
		}
		switch b {
		case OpCodeAux:
			e, err := p.parseAuxiliaryFields()
			if err != nil {
				return err
			}
			eventC <- &eventWrapper{e: e}
			continue
		case OpCodeSelectDb:
			e, err := p.parseSelectDb()
			if err != nil {
				return err
			}
			eventC <- &eventWrapper{e: e}
			continue
		case OpCodeResizeDb:
			e, err := p.parseResizeDb()
			if err != nil {
				return err
			}
			eventC <- &eventWrapper{e: e}
			continue
		case OpExpireTime:
			expireTime, err = p.parseExpireTime()
			if err != nil {
				return err
			}
			continue
		case OpExpireTimeMs:
			expireTime, err = p.parseExpireTimeMs()
			if err != nil {
				return err
			}
			continue
		case OpCodeEOF:
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

func (p *Parser) parseEntryWithValueType(valueType byte, expireAt uint64) (Event, error) {
	// TODO expires?

	key, err := p.r.GetLengthString()
	if err != nil {
		return nil, err
	}

	switch valueType {
	case ValueTypeString:
		return parseString(key, p.r)
	case ValueTypeList, ValueTypeZipList, ValueTypeListQuickList:
		return parseList(key, p.r, valueType)
	case ValueTypeSet:
		return parseSet(key, p.r)
	case ValueTypeZSetZipList:
		return parseZSet(key, p.r, valueType)
	case ValueTypeHashZipList:
		return parseHash(key, p.r, valueType)
	case ValueTypeStreamListPacks, ValueTypeListQuickList2:
		return parseStream(key, p.r, valueType)
	default:
		return nil, fmt.Errorf("unsupported rdb value type: 0x%x", valueType)
	}
}
