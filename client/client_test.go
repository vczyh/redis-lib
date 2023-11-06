package client

import "testing"

func TestClient_Auth(t *testing.T) {
	c, err := NewClient(&Config{
		Host:     "127.0.0.1",
		Port:     26379,
		Username: "",
		Password: "123",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Auth(); err != nil {
		t.Fatal(err)
	}

	if err = c.Ping(); err != nil {
		t.Fatal(err)
	}

	//if err = c.SyncWithMaster(); err != nil {
	//	t.Fatal(err)
	//}

}
