package replica

import (
	"github.com/vczyh/redis-lib/rdb"
	"io"
	"os"
	"testing"
)

func TestNewReplica_FullSync(t *testing.T) {
	r, err := NewReplica(&Config{
		MasterIP:              "127.0.0.1",
		MasterPort:            26379,
		MasterUser:            "",
		MasterPassword:        "123",
		RdbWriter:             os.Stdout,
		AofWriter:             os.Stdout,
		ContinueAfterFullSync: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.SyncWithMaster(); err != nil {
		t.Fatal(err)
	}
}

func TestReplica_Sync(t *testing.T) {
	rdbReader, rdbWriter := io.Pipe()

	r, err := NewReplica(&Config{
		MasterIP:       "127.0.0.1",
		MasterPort:     26379,
		MasterUser:     "",
		MasterPassword: "123",
		RdbWriter:      rdbWriter,
		AofWriter:      os.Stdout,
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := parseRdb(rdbReader); err != nil {
			t.Error(err)
			return
		}
	}()

	if err = r.SyncWithMaster(); err != nil {
		t.Fatal(err)
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
