package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

const (
	OK         = "OK"
	PONG       = "PONG"
	FULLRESYNC = "FULLRESYNC"
	EMPTY_FILE = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)

var commandHandlers = map[CommandType]func(*Server, *Connection, []string) (int, error){
	Ping:     ping,
	Echo:     echo,
	Set:      set,
	Get:      get,
	Info:     info,
	Config:   config,
	ReplConf: replConf,
	Psync:    psync,
}

func HandleCommand(s *Server, connection *Connection, command *Command) (int, error) {
	handler := resolveHandler(command.CommandType)
	n, err := handler(s, connection, command.Agrs)
	if err == nil {
		maybeReplicateCommand(s, command)
	}
	return n, err
}

func resolveHandler(command CommandType) func(*Server, *Connection, []string) (int, error) {
	if f, ok := commandHandlers[command]; ok {
		return f
	}
	return unknown
}

func maybeReplicateCommand(s *Server, command *Command) {
	if command.CommandType == Set && s.isMaster && len(s.asMaster.slaves) > 0 {
		log.Println("Replicating command to", len(s.asMaster.slaves), "slaves:", string(command.Buffer))
		for _, slave := range s.asMaster.slaves {
			go replicate(slave, command.Buffer)
		}
	}
}

func replicate(slave *Slave, commandBuf []byte) (int, error) {
	// TODO: better error handling
	return slave.connection.conn.Write(commandBuf)
}

func ping(s *Server, c *Connection, args []string) (int, error) {
	return c.conn.Write([]byte(resp.EncodeSimpleString(PONG)))
}

func echo(s *Server, c *Connection, args []string) (int, error) {
	return c.conn.Write([]byte(resp.EncodeBulkString(args[0])))
}

func get(s *Server, c *Connection, args []string) (int, error) {
	if len(args) < 1 {
		return c.conn.Write([]byte(resp.EncodeError(fmt.Sprintf("ERR wrong number of arguments for GET: %v", len(args)))))
	}

	v, err := s.db.Get(args[0])
	if err != nil {
		switch e := err.(type) {
		case *internal.KeyNotFoundError:
			return c.conn.Write([]byte(resp.EncodeBulkString("")))
		case *internal.KeyExpiredError:
			return c.conn.Write([]byte(resp.NULL_BULK_STRING))
		default:
			return c.conn.Write([]byte(resp.EncodeError(e.Error())))
		}
	}

	return c.conn.Write([]byte(resp.EncodeBulkString(v.Value)))
}

func set(s *Server, c *Connection, args []string) (int, error) {
	if len(args) != 2 && len(args) != 4 {
		return c.conn.Write([]byte(resp.EncodeError(fmt.Sprintf("ERR wrong number of arguments for SET: %v", len(args)))))
	}

	err := setInternal(s, args)
	if !s.isMaster && c.id == s.asSlave.masterConnection.id {
		// No need to respond to the master
		return 0, err
	}

	if err != nil {
		return c.conn.Write([]byte(resp.EncodeError(err.Error())))
	}
	return c.conn.Write([]byte(resp.EncodeSimpleString(OK)))
}

func setInternal(s *Server, args []string) error {
	key := args[0]
	val := args[1]
	var expiryMilis int64 = s.db.Options.ExpiryTime

	// resolve expiry
	if len(args) == 4 {
		expiryType := args[2]
		expiryNum, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return err
		}

		expiryNum, err = resolveExpiry(expiryType, expiryNum)
		if err != nil {
			return err
		}
		expiryMilis = expiryNum
	}

	s.db.Set(key, val, expiryMilis)
	return nil
}

func resolveExpiry(expiryType string, expiryNum int64) (int64, error) {
	switch expiryType {
	case "px":
		return expiryNum, nil
	case "ex":
		return expiryNum * 1000, nil
	default:
		return -1, fmt.Errorf("invalid expiry type")
	}
}

func config(s *Server, c *Connection, args []string) (int, error) {
	if len(args) == 0 {
		return c.conn.Write([]byte(resp.EncodeError("ERR wrong number of arguments for CONFIG commands")))
	}

	if args[0] == "get" {
		if len(args) != 2 {
			return c.conn.Write([]byte(resp.EncodeError("ERR wrong number of arguments for CONFIG GET")))
		}

		switch args[1] {
		case "dir":
			return c.conn.Write([]byte(resp.EncodeArrayBulkStrings([]string{"dir", s.db.Options.Dir})))
		case "s.dbfilename":
			return c.conn.Write([]byte(resp.EncodeArrayBulkStrings([]string{"s.dbfilename", s.db.Options.DbFilename})))
		default:
			return c.conn.Write([]byte(resp.EncodeError("ERR unknown CONFIG parameter")))
		}
	}

	return c.conn.Write([]byte(resp.EncodeError("ERR unknown CONFIG subcommand")))
}

func info(s *Server, c *Connection, args []string) (int, error) {
	var infos []string = make([]string, 0, 4)

	var role string
	if s.isMaster {
		role = "master"
	} else {
		role = "slave"
	}

	infos = append(infos,
		"role:"+role,
	)

	if len(args) == 0 {
		// TODO: add more info
	}

	if s.isMaster {
		infos = append(infos,
			"master_replid:"+s.asMaster.repl_id,
			"master_repl_offset:"+strconv.FormatInt(s.asMaster.repl_offset, 10),
		)
	}

	return c.conn.Write([]byte(resp.EncodeBulkString(strings.Join(infos, "\n"))))
}

func replConf(s *Server, c *Connection, args []string) (int, error) {
	if !s.isMaster {
		return c.conn.Write([]byte(resp.EncodeError("Not eligible to serve REPLCONF")))
	}

	if len(args) == 0 {
		return c.conn.Write([]byte(resp.EncodeError("ERR wrong number of arguments for REPLCONFIG subcommand")))
	}

	var slave *Slave
	if sl, ok := s.asMaster.slaves[c.id]; ok {
		slave = sl
	} else {
		slave = &Slave{
			connection: *c,
			capa:       make([]string, 0),
		}
		s.asMaster.slaves[c.id] = slave
	}

	switch args[0] {
	case "listening-port":
		if len(args) != 2 {
			return c.conn.Write([]byte(resp.EncodeError("ERR wrong number of arguments for REPLCONFIG listening-port subcommand")))
		}
		portStr := args[1]
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return c.conn.Write([]byte(resp.EncodeError("ERR invalid listening port")))
		}
		slave.listeningPort = port
		log.Println("Replica is listening on port:", portStr)
	case "capa":
		if len(args) < 2 {
			return c.conn.Write([]byte(resp.EncodeError("ERR wrong number of arguments for REPLCONFIG capa subcommand")))
		}
		capaStr := args[1]
		slave.capa = append(slave.capa, capaStr)
		log.Println("Replica supports:", capaStr)
	default:
		return c.conn.Write([]byte(resp.EncodeError("ERR unknown REPLCONFIG subcommand")))
	}

	return c.conn.Write([]byte(resp.EncodeSimpleString(OK)))
}

func psync(s *Server, c *Connection, args []string) (int, error) {
	if !s.isMaster {
		return c.conn.Write([]byte(resp.EncodeError("Not eligible to serve PSYNC")))
	}
	if len(args) != 2 {
		return c.conn.Write([]byte(resp.EncodeError("ERR wrong number of arguments for PSYNC")))
	}

	if _, ok := s.asMaster.slaves[c.id]; !ok {
		return 0, fmt.Errorf("slave not found")
	}

	n, err := c.conn.Write([]byte(resp.EncodeSimpleString(fmt.Sprintf("%s %s 0", FULLRESYNC, s.asMaster.repl_id))))
	if err != nil {
		return n, err
	}

	// Send empty file
	buf, err := hex.DecodeString(EMPTY_FILE)
	if err != nil {
		return n, err
	}
	fileN, err := c.conn.Write([]byte(resp.EncodeFile(buf)))

	return n + fileN, err
}

func unknown(s *Server, c *Connection, args []string) (int, error) {
	return c.conn.Write([]byte(resp.EncodeError("ERR unknown command")))
}
