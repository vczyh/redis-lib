package redis_lib

import (
	"bufio"
	"fmt"
	"net"
	"testing"
)

func TestCreateConnection(t *testing.T) {
	conn, err := net.Dial("tcp", "127.0.0.1:26379")
	if err != nil {
		t.Fatal(err)
	}

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	//_, err = w.Write([]byte("*1\r\n$5\r\nHELLO\r\n"))
	//_, err = w.Write([]byte("*2\r\n$5\r\nHELLO\r\n$16\r\nAUTH default 123\r\n"))
	_, err = w.Write([]byte("*2\r\n$4\r\nAUTH\r\n$3\r\n123\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	w.Flush()

	line, err := readLine(r)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(line))
}

func readLine(r *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		line = append(line, b)
		l := len(line)
		if l >= 2 && line[l-2] == '\r' && line[l-1] == '\n' {
			break
		}
	}
	return line, nil
}

func Test2(t *testing.T) {
	//a := -110
	//fmt.Printf("%b", a)
	//var a1 []string
	//a2 := append([]string{"a"}, a1...)
	//fmt.Println(a2)
	fmt.Println(2 << 6)
}
