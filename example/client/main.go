package main

import "github.com/vczyh/redis-lib/client"

func main() {
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
}
