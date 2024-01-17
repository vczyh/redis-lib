# Redis lib

## Features

- [Create connection with Redis server](#create-connection)
- [Parse RDB file](#parse-rdb)
- [Fake replica, sync RDB and AOF with master](#fake-replica)

## Compatibility

- Support Redis 6 / 7

## Install

```shell
go get github.com/vczyh/redis-lib
```

## Create Connection

```go  
c, err := client.NewClient(&client.Config{  
    Host:     "127.0.0.1",  
    Port:     26379,  
    Username: "",  
    Password: "123",  
})  
if err != nil {  
    panic(err)  
}  
  
if err := c.Auth(); err != nil {  
    panic(err)  
}  
  
if err = c.Ping(); err != nil {  
    panic(err)  
}
```  

## Parse RDB

```go  
p, err := rdb.NewParser("/tmp/rdb_test.rdb")  
if err != nil {  
    panic(err)  
}  
  
s, err := p.Parse()  
if err != nil {  
    panic(err)  
}  
  
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
  
if err := s.Err(); err != nil {  
    panic(err)  
}

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

## Fake Replica

```go  
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
```

synchronize data and parse RDB:

```go
rdbReader, rdbWriter := io.Pipe()  
  
r, err := replica.NewReplica(&replica.Config{  
    MasterIP:       "127.0.0.1",  
    MasterPort:     26379,  
    MasterUser:     "",  
    MasterPassword: "123",  
    RdbWriter:      rdbWriter,  
    AofWriter:      os.Stdout,  
})  
if err != nil {  
    panic(err)  
}  
  
go func() {  
    if err := parseRdb(rdbReader); err != nil {  
       panic(err)  
    }  
}()  
  
if err = r.SyncWithMaster(); err != nil {  
    panic(err)  
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
```

