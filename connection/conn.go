package connection

import (
	"bufio"
	"fmt"
	"github.com/vczyh/redis-lib/resp"
	"net"
)

type Conn struct {
	nc net.Conn
	*bufio.Reader
	*bufio.Writer
}

func NewConn(nc net.Conn) (*Conn, error) {
	c := &Conn{
		nc:     nc,
		Reader: bufio.NewReader(nc),
		Writer: bufio.NewWriter(nc),
	}
	return c, nil
}

func (c *Conn) ReadString() (string, error) {
	return resp.ReadString(c.Reader)
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
	if err := resp.WriteArray(c, array...); err != nil {
		return err
	}
	return c.Flush()
}

func (c *Conn) WriteArray(args ...string) error {
	if err := resp.WriteArray(c, args...); err != nil {
		return err
	}
	return c.Flush()
}

func (c *Conn) WriteBulkString(str string) error {
	if err := resp.WriteBulkString(c, str); err != nil {
		return err
	}
	return c.Flush()
}

func (c *Conn) ReadData() ([]byte, error) {
	return resp.ReadData(c.Reader)
}

func (c *Conn) Close() error {
	return c.nc.Close()
}
