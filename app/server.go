package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	masterHost       string
	masterPort       int
	masterConnection *Connection
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
		connection, err := syncWithMaster(s)
		if err != nil {
			log.Println("Error syncing with master:", err)
			os.Exit(1)
		}
		log.Println("Done syncing with master:", s.asSlave.masterHost, s.asSlave.masterPort)
		s.asSlave.masterConnection = connection
	}

	addr := fmt.Sprintf("0.0.0.0:%d", s.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	log.Println("Listening on:", addr)

	if !s.isMaster {
		// Serving master connection after this slave is up and listening
		go s.handleConnection(s.asSlave.masterConnection)
		time.Sleep(1 * time.Second) // waiting for getting propagated keys from master
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		connection := NewConnection(getConnID(), conn)
		go s.handleConnection(connection)
	}
}

func (server *Server) handleConnection(c *Connection) {
	defer c.conn.Close()
	// TODO: clean closed slave connection

	log.Println("Received connection from:", c.conn.RemoteAddr())
	reader := bufio.NewReader(c.conn)

	for {
		rp, err := resp.ReadNextResp(reader)
		if err != nil {
			if err == io.EOF {
				log.Println("Client closed connection")
				break
			}
			log.Fatalln("Error reading RESP", err)
			continue
		}

		command, err := ParseCommandFromRESP(rp)
		if err != nil {
			log.Fatalf("Error parsing command from client: %v - Command\n: %v", err, command)
			continue
		}

		log.Printf("SERVER: Client %v sent command: %v - %v\n", c.conn.RemoteAddr(), command.CommandType, command.Agrs)

		_, err = HandleCommand(server, c, command)
		if err != nil {
			log.Fatalf("Error handling command %v: %v", command, err)
			continue
		}
	}
}

func generateReplId() string {
	return "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
}

var connID int64

func getConnID() ConnectionID {
	return ConnectionID(atomic.AddInt64(&connID, 1))
}
