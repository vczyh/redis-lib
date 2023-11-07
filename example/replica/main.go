package main

import (
	"github.com/vczyh/redis-lib/replica"
	"os"
)

func main() {
	r, err := replica.NewReplica(&replica.Config{
		MasterIP:            "127.0.0.1",
		MasterPort:          26379,
		MasterUser:          "",
		MasterPassword:      "123",
		MasterReplicaOffset: 67528,
		RdbWriter:           os.Stdout,
		AofWriter:           os.Stdout,
	})
	if err != nil {
		panic(err)
	}

	if err := r.SyncWithMaster(); err != nil {
		panic(err)
	}
}
