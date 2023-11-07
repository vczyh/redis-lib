## Redis lib

- 作为客户端，与`Redis Server`通信
- 解析`RDB`
- 作为`Replica`，从`Master`同步数据

### 客户端

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

### 解析 RDB

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

### 同步数据

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

### 同步数据并解析 RDB

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

