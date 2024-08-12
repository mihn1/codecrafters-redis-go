package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/codecrafters-io/redis-starter-go/internal"
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
	asMaster AsMasterInfo
	asSlave  AsSlaveInfo
	mu       *sync.Mutex
}

type AsMasterInfo struct {
	repl_id     string
	repl_offset int64
	slaves      map[ConnectionID]*Slave
}

type Slave struct {
	connection    Connection
	listeningPort int
	capa          []string
}

type AsSlaveInfo struct {
	masterHost string
	masterPort int
}

func NewServer(options ServerOptions) *Server {
	server := &Server{
		port: options.Port,
		mu:   &sync.Mutex{},
	}

	if options.Replicaof == "" {
		server.isMaster = true
		server.asMaster.repl_id = generateReplId()
		server.asMaster.repl_offset = 0
		server.asMaster.slaves = make(map[ConnectionID]*Slave)
	} else {
		server.isMaster = false
		splitted := strings.Split(options.Replicaof, " ")
		if len(splitted) != 2 {
			log.Println("Invalid replicaof format")
			os.Exit(1)
		}
		server.asSlave.masterHost = splitted[0]
		port, err := strconv.Atoi(splitted[1])
		if err != nil {
			log.Println("Invalid master port:", err)
			os.Exit(1)
		}
		server.asSlave.masterPort = port
	}

	server.db = internal.NewDB(internal.DBOptions{Dir: options.Dir, DbFilename: options.DbFilename})
	return server
}

func (s *Server) Run() {
	if !s.isMaster {
		// sync with master
		err := syncWithMaster(s)
		if err != nil {
			log.Println("Error syncing with master:", err)
			os.Exit(1)
		}
	}

	addr := fmt.Sprintf("0.0.0.0:%d", s.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	log.Println("Listening on:", addr)

	var connID int64
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		atomic.AddInt64(&connID, 1)
		connection := NewConnection(ConnectionID(connID), conn)
		go s.handleConnection(connection)
	}
}

func (server *Server) handleConnection(c *Connection) {
	defer c.conn.Close()
	// TODO: clean closed slave connection

	log.Println("Received connection from:", c.conn.RemoteAddr())
	buf := make([]byte, 1024)

	for {
		n, err := c.conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("Error reading", err)
		}

		command, err := ParseCommand(buf[:n])
		if err != nil {
			log.Fatalf("Error parsing command from client: %v", err)
			continue
		}

		log.Printf("SERVER: Client %v sent command: %v - %v\n", c.conn.RemoteAddr(), command.CommandType, command.Agrs)

		_, err = HandleCommand(server, c, command)
		if err != nil {
			log.Fatalf("Error handling command: %v", err)
			continue
		}
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
