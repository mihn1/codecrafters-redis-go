package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

type ServerOptions struct {
	Replicaof  string
	DbFilename string
	Dir        string
	Port       int
}

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
	masterHost string
	masterPort int
}

func NewServer(options ServerOptions) *Server {
	server := &Server{
		port: options.Port,
	}

	if options.Replicaof == "" {
		server.isMaster = true
		server.master.replid = generateReplId()
		server.master.repl_offset = 0
	} else {
		server.isMaster = false
		splitted := strings.Split(options.Replicaof, " ")
		if len(splitted) != 2 {
			log.Println("Invalid replicaof format")
			os.Exit(1)
		}
		server.slave.masterHost = splitted[0]
		port, err := strconv.Atoi(splitted[1])
		if err != nil {
			log.Println("Invalid master port:", err)
			os.Exit(1)
		}
		server.slave.masterPort = port
	}

	server.db = internal.NewDB(internal.DBOptions{Dir: options.Dir, DbFilename: options.DbFilename})
	return server
}

func (s *Server) Run() {
	if !s.isMaster {
		// sync to master
		syncWithMaster(s)
	}

	addr := fmt.Sprintf("0.0.0.0:%d", s.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	log.Println("Listening on:", addr)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go s.handleConnection(conn)
	}
}

func (server *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	log.Println("Received connection from:", conn.RemoteAddr())
	buf := make([]byte, 1024)

	for {
		_, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("Error reading", err)
		}

		data := bytes.Trim(buf, "\x00")
		message := string(data)
		fmt.Printf("Client %v sent message: %v\n", conn.RemoteAddr(), message)

		command, err := ParseCommand(message)
		if err != nil {
			log.Println("Error parsing command", err)
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
	// 	log.Println("Error generating repl id", err)
	// 	os.Exit(1)
	// }
	// base64Encode := base64.StdEncoding.EncodeToString(buf)
	// id := fmt.Sprintf("%x", base64Encode)
	// return id
}

func syncWithMaster(s *Server) error {
	err := sendHandshake(s)
	return err
}

func sendHandshake(s *Server) error {
	masterAddr := fmt.Sprintf("%s:%d", s.slave.masterHost, s.slave.masterPort)
	log.Println("Syncing...", masterAddr)
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		log.Fatalf("Error connecting to master: %v", err)
		return err
	}

	return sendPing(s, conn)
}

func sendPing(s *Server, conn net.Conn) error {
	message := resp.EncodeArrayBulkStrings([]string{"PING"})
	_, err := conn.Write([]byte(message))
	if err != nil {
		return err
	}

	res, err := io.ReadAll(conn)
	if err != nil {
		return err
	}

	log.Printf("Received response: %s", string(res))
	return nil
}
