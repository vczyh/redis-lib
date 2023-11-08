package main

import (
	"github.com/vczyh/redis-lib/rdb"
	"github.com/vczyh/redis-lib/replica"
	"io"
	"os"
)

func main() {
	rdbReader, rdbWriter := io.Pipe()

	r, err := replica.NewReplica(&replica.Config{
		MasterIP:       "127.0.0.1",
		MasterPort:     26379,
		MasterUser:     "",
		MasterPassword: "123",
		RdbWriter:      rdbWriter,
		AofWriter:      os.Stdout,
		//ContinueAfterFullSync: true,

		MasterReplicaId:         "aaa",
		MasterReplicaOffset:     22,
		ContinueIfPartialFailed: true,
	})
	if err != nil {
		panic(err)
	}

	go func() {
		// Parse rbd from master.
		if err := parseRdb(rdbReader); err != nil {
			panic(err)
		}
	}()

	if err = r.SyncWithMaster(); err != nil {
		panic(err)
	}
}
func parseRdb(r io.Reader) error {
	p, err := rdb.NewReaderParser(r)
	if err != nil {
		return err
	}
	s, err := p.Parse()
	if err != nil {
		return err
	}
	for s.HasNext() {
		e := s.Next()
		e.Event.Debug()
	}
	return s.Err()
}
