package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/util"
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
	connection    *Connection
	listeningPort int
	capa          []string
	syncOffset    int64 // Offset sent by the master
	ackOffset     int64 // Offset acked by the slave through GETACk - ACK commands
}

type AsSlaveInfo struct {
	masterHost       string
	masterPort       int
	masterConnection *Connection
	offset           int64
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
	// Load the RDB file -> has to be executed first
	s.loadRDB()

	if !s.isMaster {
		// sync with master after the server is up the running
		masterConnection, err := syncWithMaster(s)
		if err != nil {
			log.Fatalln("Error syncing with master:", err)
			os.Exit(1)
		}
		log.Println("Done syncing with master:", s.asSlave.masterHost, s.asSlave.masterPort)
		// Serving master connection after this slave is up and listening
		log.Println("Handling master connection")
		s.asSlave.masterConnection = masterConnection
		go s.handleConnection(s.asSlave.masterConnection)
		time.Sleep(2 * time.Second) // waiting for getting propagated keys from master
	}

	addr := fmt.Sprintf("0.0.0.0:%d", s.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Println("Failed to bind to port", s.port)
		os.Exit(1)
	}
	defer l.Close()
	log.Println("Listening on:", addr)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			continue
		}

		connection := NewConnection(getConnID(), conn)
		go s.handleConnection(connection)
	}
}

func (s *Server) handleConnection(c *Connection) {
	defer func() {
		c.conn.Close()
		// TODO: move this logic to be handled by the master struct
		if s.isMaster {
			s.mu.Lock()
			delete(s.asMaster.slaves, c.id)
			s.mu.Unlock()
		}
	}()

	log.Println("Handling connection from:", c.conn.RemoteAddr())
	closed := false

	for !closed {
		rp, err := resp.ReadNextResp(c.reader)
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

		log.Printf("SERVER: Client %v sent command: %v - %v\n", c.conn.RemoteAddr(), command.CommandType, command.Args)

		_, err = HandleCommand(s, c, command)
		if err != nil {
			log.Fatalf("Error handling command %v: %v", command, err)
			continue
		}
	}
}

func (s *Server) loadRDB() {
	if util.IsEmptyOrWhitespace(s.db.Options.Dir) || util.IsEmptyOrWhitespace(s.db.Options.DbFilename) {
		return
	}

	rdbPath := filepath.Join(s.db.Options.Dir, s.db.Options.DbFilename)
	rdbReader := internal.NewRDBReader()
	data, err := rdbReader.LoadFile(rdbPath)
	if err != nil {
		log.Fatal("Can't load the provided RDB file")
		panic(err)
	}
	if data == nil {
		log.Println("Starting a clean DB")
		return
	}

	log.Printf("Initing db with %d keys\n", len(data))
	s.db.InitStorage(data)
}

func generateReplId() string {
	return "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
}

var connID int64

func getConnID() ConnectionID {
	return ConnectionID(atomic.AddInt64(&connID, 1))
}
