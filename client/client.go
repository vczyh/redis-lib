package client

import (
	"fmt"
	"net"
	"redis-lib/connection"
	"strconv"
)

type Client struct {
	conn   *connection.Conn
	nc     net.Conn
	config *Config
}

type Config struct {
	Host     string
	Port     int
	Username string
	Password string
}

func NewClient(config *Config) (*Client, error) {
	c := &Client{
		config: config,
	}
	nc, err := net.Dial("tcp", net.JoinHostPort(config.Host, strconv.Itoa(config.Port)))
	if err != nil {
		return nil, err
	}
	c.nc = nc

	conn, err := connection.NewConn(nc)
	if err != nil {
		return nil, err
	}
	c.conn = conn

	return c, nil
}

func (c *Client) Auth() error {
	var args []string
	if c.config.Username != "" {
		args = append(args, c.config.Username)
	}
	args = append(args, c.config.Password)

	if err := c.conn.WriteCommand("AUTH", args...); err != nil {
		return err
	}
	return c.conn.SkipOk()
}

func (c *Client) Ping() error {
	if err := c.conn.WriteCommand("PING"); err != nil {
		return err
	}
	res, err := c.conn.ReadString()
	if err != nil {
		return err
	}
	if res != "PONG" {
		return fmt.Errorf("PING response not PONG: %s", res)
	}
	return nil
}

func (c *Client) Conn() *connection.Conn {
	return c.conn
}

func (c *Client) Close() error {
	return c.conn.Close()
}
