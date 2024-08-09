package main

import (
	"bufio"
	"fmt"
	"io"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	fmt.Println("Received connection from: ", conn.RemoteAddr())
	reader := bufio.NewReader(conn)
	buf := make([]byte, 1024)

	for {
		_, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Println("Error reading", err)
		}

		message := string(buf)
		fmt.Println("Received message:", message)

		res := "+PONG\r\n"
		conn.Write([]byte(res))
	}
}
