package replica

import (
	"bytes"
	"fmt"
	"github.com/vczyh/redis-lib/client"
	"io"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Replica struct {
	config *Config
	client *client.Client

	replicaId     string
	replicaOffset atomic.Int64
}

type Config struct {
	MasterIP       string
	MasterPort     int
	MasterUser     string
	MasterPassword string

	AnnounceIP   string
	AnnouncePort int

	// Send command (PSYNC MasterReplicaId MasterReplicaOffset) to master.
	// Replica will start a partial synchronization if replicaId and offset exist in master.
	// Set ContinueIfPartialFailed true if you hope do a full synchronized after partial synchronization failed.
	MasterReplicaId     string
	MasterReplicaOffset int

	// Whether to do a full synchronized after partial synchronization failed.
	ContinueIfPartialFailed bool

	// Receive RDB from master in full synchronization.
	RdbWriter io.Writer

	// Whether to continue incremental synchronization(AOF) after full synchronization.
	ContinueAfterFullSync bool

	// Receive AOF bytes stream after full synchronization if ContinueAfterFullSync is true.
	// Receive all AOF bytes stream in partial synchronization.
	AofWriter io.Writer
}

func NewReplica(config *Config) (*Replica, error) {
	r := &Replica{
		config: config,
	}
	return r, nil
}

// SyncWithMaster establish a connection with the master, and sync data from master.
// replication.c::syncWithMaster
func (r *Replica) SyncWithMaster() error {
	// Create connection with master.
	if err := r.createClient(); err != nil {
		return err
	}

	if err := r.client.Auth(); err != nil {
		return err
	}

	// Check for errors in the socket: after a non blocking connect() we may find that the socket is in error state.
	if err := r.client.Ping(); err != nil {
		return err
	}

	conn := r.client.Conn()
	if port := r.config.AnnouncePort; port != 0 {
		if err := conn.WriteCommand("REPLCONF", "listening-port", strconv.Itoa(port)); err != nil {
			return err
		}
		if err := conn.SkipOk(); err != nil {
			return err
		}
	}
	if ip := r.config.AnnounceIP; ip != "" {
		if err := conn.WriteCommand("REPLCONF", "ip-address", ip); err != nil {
			return err
		}
		if err := conn.SkipOk(); err != nil {
			return err
		}
	}

	// Inform the master of our (slave) capabilities.
	//
	// EOF: supports EOF-style RDB transfer for diskless replication.
	// PSYNC2: supports PSYNC v2, so understands +CONTINUE <new repl ID>.
	//
	// The master will ignore capabilities it does not understand.
	// TODO  eof
	if err := conn.WriteCommand("REPLCONF", "capa", "eof", "capa", "psync2"); err != nil {
		return err
	}
	if err := conn.SkipOk(); err != nil {
		return err
	}

	// Send PSYNC command.
	//
	// Full sync: PSYNC ? 01
	// Partial sync: PSYNC replicaId offset
	replicaId := "?"
	offset := -1
	partial := r.config.MasterReplicaId != "" && r.config.MasterReplicaOffset > 0
	if masterReplicaId := r.config.MasterReplicaId; masterReplicaId != "" {
		replicaId = masterReplicaId
	}
	if masterOffset := r.config.MasterReplicaOffset; masterOffset != 0 {
		offset = masterOffset
	}
	if err := conn.WriteCommand("PSYNC", replicaId, strconv.Itoa(offset)); err != nil {
		return err
	}
	reply, err := r.receiveSynchronousResponse()
	if err != nil {
		return err
	}

	// Write offset ack and keepalive.
	go func() {
		if err := r.sendOffsetAckTicker(); err != nil {
			fmt.Printf("fail send ack to master: %s\n", err)
			return
		}
	}()

	switch {
	case strings.HasPrefix(reply, "FULLRESYNC"):
		split := strings.Split(reply, " ")
		if len(split) != 3 {
			return fmt.Errorf("PSYNC FULLRESYNC response format invalid: %s", reply)
		}
		r.replicaId = split[1]
		offsetInt, err := strconv.Atoi(split[2])
		if err != nil {
			return err
		}

		if partial && !r.config.ContinueIfPartialFailed {
			return fmt.Errorf("master tells you that you need do a full synchroinzation")
		}

		if err := r.fullSync(offsetInt); err != nil {
			return err
		}
	case strings.HasPrefix(reply, "CONTINUE"):
		r.replicaId = replicaId
		split := strings.Split(reply, " ")
		if len(split) >= 2 {
			r.replicaId = split[1]
		}
		r.replicaOffset.Store(int64(offset))
		if err := r.partialSync(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported PSYNC commadn response: %s", reply)
	}

	return nil
}

// replication.c::receiveSynchronousResponse
func (r *Replica) receiveSynchronousResponse() (string, error) {
	// Read the reply from the server.
	conn := r.client.Conn()
	for {
		bytes, err := conn.Peek(1)
		if err != nil {
			return "", err
		}
		if bytes[0] != '\n' {
			break
		} else {
			if _, err := conn.Discard(1); err != nil {
				return "", err
			}
		}
	}
	return conn.ReadString()
}

func (r *Replica) Close() error {
	return r.client.Close()
}

func (r *Replica) partialSync() error {
	buf := make([]byte, 10*1024)
	for {
		n, err := r.client.Conn().Read(buf)
		if err != nil {
			return err
		}
		if _, err = r.config.AofWriter.Write(buf[:n]); err != nil {
			return err
		}
		r.replicaOffset.Add(int64(n))
	}
}

func (r *Replica) fullSync(offset int) error {
	conn := r.client.Conn()

	for {
		bs, err := conn.Peek(1)
		if err != nil {
			return err
		}
		if bs[0] != '\n' {
			break
		} else {
			if _, err := conn.Discard(1); err != nil {
				return err
			}
		}
	}
	replayData, err := conn.ReadData()
	if err != nil {
		return err
	}
	if len(replayData) == 0 {
		/* At this stage just a newline works as a PING in order to take
		 * the connection live. So we refresh our last interaction
		 * timestamp. */
		return nil
	} else if replayData[0] != '$' {
		return fmt.Errorf("bad protocol from MASTER, the first byte is not '$' (we received '%s'), are you sure the host and port are right", replayData)
	}
	reply := string(replayData[1:])

	// Static vars used to hold the EOF mark, and the last bytes received
	// from the server: when they match, we reached the end of the transfer.
	eofMark := make([]byte, 40)
	lastBytes := make([]byte, 40)
	lastBytesSize := 0
	transferSize := 0
	var useMark bool
	if strings.HasPrefix(reply, "EOF") {
		useMark = true
		copy(eofMark, reply[4:])
		transferSize = 0
	} else {
		useMark = false
		transferSize, err = strconv.Atoi(reply)
		if err != nil {
			return err
		}
	}

	bufSize := 10 * 1024 * 1024
	buf := make([]byte, bufSize)

	if useMark {
		for {
			n, err := conn.Read(buf)
			if err != nil {
				return err
			}

			if n > 40 {
				if _, err = r.config.RdbWriter.Write(lastBytes[:lastBytesSize]); err != nil {
					return err
				}
				copy(lastBytes, buf[n-40:])
				lastBytesSize = 40
				if _, err = r.config.RdbWriter.Write(buf[:n-40]); err != nil {
					return err
				}
			} else {
				if lastBytesSize+n > 40 {
					if _, err = r.config.RdbWriter.Write(lastBytes[:lastBytesSize+n-40]); err != nil {
						return err
					}
				}
				copy(lastBytes, lastBytes[n:])
				copy(lastBytes[40-n:], buf[:n])
				lastBytesSize = int(math.Min(float64(n+lastBytesSize), 40))
			}

			if bytes.Equal(lastBytes, eofMark) {
				break
			}
		}
	} else {
		unReadSize := transferSize
		for unReadSize > 0 {
			needSize := bufSize
			if unReadSize < needSize {
				needSize = unReadSize
			}
			n, err := conn.Read(buf[:needSize])
			if err != nil {
				return err
			}
			unReadSize -= n
			if _, err = r.config.RdbWriter.Write(buf[:n]); err != nil {
				return err
			}
		}
	}

	r.replicaOffset.Store(int64(offset))

	if r.config.ContinueAfterFullSync {
		if err = r.syncAOF(); err != nil {
			return err
		}
	}

	return nil
}

func (r *Replica) syncAOF() error {
	buf := make([]byte, 1024*100)
	for {
		n, err := r.client.Conn().Read(buf)
		if err != nil {
			return err
		}
		if _, err = r.config.AofWriter.Write(buf[:n]); err != nil {
			return err
		}
		r.replicaOffset.Add(int64(n))
	}
}

func (r *Replica) sendOffsetAckTicker() error {
	t := time.Tick(1 * time.Second)
	for range t {
		offset := r.replicaOffset.Load()
		if offset > 0 {
			if err := r.client.Conn().WriteCommand("REPLCONF", "ACK", strconv.FormatInt(offset, 10)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Replica) createClient() error {
	c, err := client.NewClient(&client.Config{
		Host:     r.config.MasterIP,
		Port:     r.config.MasterPort,
		Username: r.config.MasterUser,
		Password: r.config.MasterPassword,
	})
	if err != nil {
		return err
	}
	r.client = c
	return nil
}
