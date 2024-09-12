# Redis lib

## Features

- [Create connection with Redis server](#creating-connection)
- [Parse RDB file](#parsing-rdb)
- [Fake replica, sync RDB and AOF with master](#faking-replica)

## Compatibility

- Support Redis 6 / 7

## Installing

```shell
go get github.com/vczyh/redis-lib
```

## Creating Connection

```go  
c, _ := client.NewClient(&client.Config{  
    Host:     "127.0.0.1",  
    Port:     26379,  
    Username: "",  
    Password: "123",  
})  

_ = c.Auth()

_ = c.Ping()
```  

## Parsing RDB

```go  
p, _ := rdb.NewParser("/tmp/rdb_test.rdb")  

s, _ := p.Parse()  

for s.HasNext() {  
    e := s.Next()  
  
    switch e.EventType {  
    case rdb.EventTypeVersion:  
       e.Event.Debug()  
    case rdb.EventTypeStringObject:  
       e.Event.Debug()  
    case rdb.EventTypeSetObject:  
       e.Event.Debug()  
    }  
}  
  
_ = s.Err()

=== VersionEvent ===
9

=== StringObjectEvent ===
Key: b
Value: 3

=== SetObjectEvent ===
Key: key:set
Size: 4
Members:
        s2
        s5
        s4
        s1
        
...
```  

## Faking Replica

```go  
r, _ := replica.NewReplica(&replica.Config{  
    MasterIP:            "127.0.0.1",  
    MasterPort:          26379,  
    MasterUser:          "",  
    MasterPassword:      "123",  
    MasterReplicaOffset: 67528,  
    RdbWriter:           os.Stdout,  
    AofWriter:           os.Stdout,  
})  
_ = r.SyncWithMaster()
```

synchronize data and parse RDB:

```go
rdbReader, rdbWriter := io.Pipe()  
  
r, _ := replica.NewReplica(&replica.Config{  
    MasterIP:       "127.0.0.1",  
    MasterPort:     26379,  
    MasterUser:     "",  
    MasterPassword: "123",  
    RdbWriter:      rdbWriter,  
    AofWriter:      os.Stdout,  
})  
  
go func() {  
    _ = parseRdb(rdbReader)
}()  
  
_ = r.SyncWithMaster()

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
```

