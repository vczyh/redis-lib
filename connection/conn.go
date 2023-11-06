package connection

import (
	"bufio"
	"fmt"
	"net"
	"redis-lib/resp"
)

type Conn struct {
	nc net.Conn
	r  *bufio.Reader
	w  *bufio.Writer
}

func NewConn(nc net.Conn) (*Conn, error) {
	c := &Conn{
		nc: nc,
		r:  bufio.NewReader(nc),
		w:  bufio.NewWriter(nc),
	}
	return c, nil
}

func (c *Conn) ReadString() (string, error) {
	return resp.ReadString(c.r)
}

func (c *Conn) SkipOk() error {
	v, err := c.ReadString()
	if err != nil {
		return err
	}
	if v != "OK" {
		return fmt.Errorf("response not OK")
	}
	return nil
}

func (c *Conn) WriteCommand(command string, args ...string) error {
	array := []string{command}
	array = append(array, args...)
	if err := resp.WriteArray(c.w, array...); err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *Conn) WriteArray(args ...string) error {
	if err := resp.WriteArray(c.w, args...); err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *Conn) WriteBulkString(str string) error {
	if err := resp.WriteBulkString(c.w, str); err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *Conn) ReadData() ([]byte, error) {
	return resp.ReadData(c.r)
}

func (c *Conn) ReadSeparatedBytes() ([]byte, error) {
	return resp.ReadSeparatedBytes(c.r)
}

func (c *Conn) Read(b []byte) (int, error) {
	return c.r.Read(b)
}

func (c *Conn) Close() error {
	return c.nc.Close()
}
