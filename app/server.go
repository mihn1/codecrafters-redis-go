package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/internal"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

var db *internal.DB

func main() {
	addr := "0.0.0.0:6379"

	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	fmt.Println("Listening on:", addr)

	db = internal.NewDB(internal.DBOptions{})

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	fmt.Println("Received connection from:", conn.RemoteAddr())
	buf := make([]byte, 1024)

	for {
		_, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading", err)
		}

		data := bytes.Trim(buf, "\x00")
		message := string(data)
		fmt.Printf("Client %v sent message: %v\n", conn.RemoteAddr(), message)

		command, err := resp.ParseCommand(message)
		if err != nil {
			fmt.Println("Error parsing command", err)
			continue
		}

		res := resp.HandleCommand(db, command)
		conn.Write([]byte(res))
	}
}
