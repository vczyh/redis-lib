package rdb

import "fmt"

type EventType uint8

const (
	EventTypeMagicNumber EventType = iota
	EventTypeVersion
	EventTypeAuxField
	EventTypeSelectDb
	EventTypeResizeDb
	EventTypeStringObject
	EventTypeListObject
	EventTypeSetObject
	EventTypeZSetObject
	EventTypeHashObject
	EventTypeStreamObject
)

type RedisRdbEvent struct {
	EventType EventType
	Event     Event
}

type Event interface {
	Debug()
}

type MagicNumberEvent struct {
	MagicNumber []byte
}

func (e *MagicNumberEvent) Debug() {
	fmt.Printf("=== MagicNumberEvent ===\n")
	fmt.Printf("%s\n", string(e.MagicNumber))
	fmt.Printf("\n")
}

type VersionEvent struct {
	Version int
}

func (e *VersionEvent) Debug() {
	fmt.Printf("=== VersionEvent ===\n")
	fmt.Printf("%d\n", e.Version)
	fmt.Printf("\n")
}

type AuxFieldEvent struct {
	Filed string
	Value string
}

func (e *AuxFieldEvent) Debug() {
	fmt.Printf("=== AuxFieldEvent ===\n")
	fmt.Printf("%s: %s\n", e.Filed, e.Value)
	fmt.Printf("\n")
}

type SelectDbEvent struct {
	Db int
}

func (e *SelectDbEvent) Debug() {
	fmt.Printf("=== SelectDbEvent ===\n")
	fmt.Printf("Database: %d\n", e.Db)
	fmt.Printf("\n")
}

type ResizeDbEvent struct {
	dbSize       int
	dbExpireSize int
}

func (e *ResizeDbEvent) Debug() {
	fmt.Printf("=== ResizeDbEvent ===\n")
	fmt.Printf("Database size: %d\n", e.dbSize)
	fmt.Printf("Database expire size: %d\n", e.dbExpireSize)
	fmt.Printf("\n")
}
