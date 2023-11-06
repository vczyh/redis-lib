## Redis Lib

- 作为客户端，与`Redis Server`通信
- 解析`RDB`
- 作为`Replica`，从`Master`同步数据

### 客户端

```go  
c, err := NewClient(&Config{  
    Host:     "127.0.0.1",  
    Port:     26379,  
    Username: "",  
    Password: "123",  
})  
if err != nil {  
    // error
}  
  
if err := c.Auth(); err != nil { 
	// error
}  
  
if err = c.Ping(); err != nil {  
    // error
}
```  

### 解析 RDB

```go  
p, err := NewParser("/tmp/test.rdb")  
if err != nil {   
    // error  
}  
  
s, err := p.Parse()  
if err != nil {  
    // error}  
  
for s.HasNext() {  
    e := s.Next()    
    e.Debug()
}  
  
if err := s.Err(); err != nil {   
    // error  
}  
```  

### 同步数据

```go  
r, err := NewReplica(&Config{  
    MasterIP:            "127.0.0.1",  
    MasterPort:          26379,  
    MasterUser:          "",  
    MasterPassword:      "123",  
    MasterReplicaOffset: 67528,  
    RdbWriter:           os.Stdout,  
    AofWriter:           os.Stdout,  
})  
if err != nil {  
    // error
}  
if err := r.SyncWithMaster(); err != nil {  
    // error
}
```