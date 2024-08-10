package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/internal"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

type Server struct {
	db   *internal.DB
	port int
}

func (s *Server) run() {
	addr := fmt.Sprintf("0.0.0.0:%d", s.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	fmt.Println("Listening on:", addr)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go s.handleConnection(conn)
	}
}

func (server *Server) handleConnection(conn net.Conn) {
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

		res := resp.HandleCommand(server.db, command)
		conn.Write([]byte(res))
	}
}

func main() {
	portStr := flag.String("port", "6379", "Port to listen on")
	dir := flag.String("dir", "/tmp/redis-files", "Directory to store RDB files")
	dbFileName := flag.String("dbfilename", "dump.rdb", "Name of the RDB file")

	flag.Parse()

	port, err := strconv.Atoi(*portStr)
	if err != nil {
		fmt.Println("Error parsing port:", err)
		os.Exit(1)
	}

	server := &Server{port: port}
	server.db = internal.NewDB(internal.DBOptions{Dir: *dir, DbFilename: *dbFileName})
	server.run()
}
