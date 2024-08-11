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
)

type Server struct {
	db       *internal.DB
	port     int
	isMaster bool
	master   MasterInfo
	slave    SlaveInfo
}

type MasterInfo struct {
	replid      string
	repl_offset int64
}

type SlaveInfo struct {
	replicaof string
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

		command, err := ParseCommand(message)
		if err != nil {
			fmt.Println("Error parsing command", err)
			continue
		}

		res := HandleCommand(server, command)
		conn.Write([]byte(res))
	}
}

func generateReplId() string {
	return "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	// generate 30 random bytes -> base64 -> 40-character random string
	// buf := make([]byte, 30)
	// _, err := rand.Read(buf)
	// if err != nil {
	// 	fmt.Println("Error generating repl id", err)
	// 	os.Exit(1)
	// }

	// base64Encode := base64.StdEncoding.EncodeToString(buf)
	// id := fmt.Sprintf("%x", base64Encode)
	// return id
}

func main() {
	portStr := flag.String("port", "6379", "Port to listen on")
	dir := flag.String("dir", "/tmp/redis-files", "Directory to store RDB files")
	replicaof := flag.String("replicaof", "", "Replica of host:port")
	dbFileName := flag.String("dbfilename", "dump.rdb", "Name of the RDB file")

	flag.Parse()

	port, err := strconv.Atoi(*portStr)
	if err != nil {
		fmt.Println("Error parsing port:", err)
		os.Exit(1)
	}

	server := &Server{
		port: port,
	}

	if *replicaof == "" {
		server.isMaster = true
		server.master.replid = generateReplId()
		server.master.repl_offset = 0
	} else {
		server.isMaster = false
		server.slave.replicaof = *replicaof
	}

	server.db = internal.NewDB(internal.DBOptions{Dir: *dir, DbFilename: *dbFileName})
	server.run()
}
