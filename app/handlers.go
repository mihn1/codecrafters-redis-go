package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"net"
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

var commandHandlers = map[CommandType]func(*Server, net.Conn, []string) (int, error){
	Ping:     ping,
	Echo:     echo,
	Set:      set,
	Get:      get,
	Info:     info,
	Config:   config,
	ReplConf: replConf,
	Psync:    psync,
}

func HandleCommand(s *Server, c net.Conn, command Command) (int, error) {
	handler := resolveHandler(command.CommandType)
	return handler(s, c, command.Agrs)
}

func resolveHandler(command CommandType) func(*Server, net.Conn, []string) (int, error) {
	if f, ok := commandHandlers[command]; ok {
		return f
	}
	return unknown
}

func ping(s *Server, c net.Conn, args []string) (int, error) {
	return c.Write([]byte(resp.EncodeSimpleString(PONG)))
}

func echo(s *Server, c net.Conn, args []string) (int, error) {
	return c.Write([]byte(resp.EncodeBulkString(args[0])))
}

func get(s *Server, c net.Conn, args []string) (int, error) {
	if len(args) < 1 {
		return c.Write([]byte(resp.EncodeError(fmt.Sprintf("ERR wrong number of arguments for GET: %v", len(args)))))
	}

	v, err := s.db.Get(args[0])
	if err != nil {
		switch e := err.(type) {
		case *internal.KeyNotFoundError:
			return c.Write([]byte(resp.EncodeBulkString("")))
		case *internal.KeyExpiredError:
			return c.Write([]byte(resp.NULL_BULK_STRING))
		default:
			return c.Write([]byte(resp.EncodeError(e.Error())))
		}
	}

	return c.Write([]byte(resp.EncodeBulkString(v.Value)))
}

func set(s *Server, c net.Conn, args []string) (int, error) {
	if len(args) != 2 && len(args) != 4 {
		return c.Write([]byte(resp.EncodeError(fmt.Sprintf("ERR wrong number of arguments for SET: %v", len(args)))))
	}

	key := args[0]
	val := args[1]
	var expiryMilis int64 = s.db.Options.ExpiryTime

	// resolve expiry
	if len(args) == 4 {
		expiryType := args[2]
		expiryNum, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return c.Write([]byte(resp.EncodeError(err.Error())))
		}

		expiryNum, err = resolveExpiry(expiryType, expiryNum)
		if err != nil {
			return c.Write([]byte(resp.EncodeError(err.Error())))
		}
		expiryMilis = expiryNum
	}

	s.db.Set(key, val, expiryMilis)
	return c.Write([]byte(resp.EncodeSimpleString(OK)))
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

func config(s *Server, c net.Conn, args []string) (int, error) {
	if len(args) == 0 {
		return c.Write([]byte(resp.EncodeError("ERR wrong number of arguments for CONFIG commands")))
	}

	if args[0] == "get" {
		if len(args) != 2 {
			return c.Write([]byte(resp.EncodeError("ERR wrong number of arguments for CONFIG GET")))
		}

		switch args[1] {
		case "dir":
			return c.Write([]byte(resp.EncodeArrayBulkStrings([]string{"dir", s.db.Options.Dir})))
		case "s.dbfilename":
			return c.Write([]byte(resp.EncodeArrayBulkStrings([]string{"s.dbfilename", s.db.Options.DbFilename})))
		default:
			return c.Write([]byte(resp.EncodeError("ERR unknown CONFIG parameter")))
		}
	}

	return c.Write([]byte(resp.EncodeError("ERR unknown CONFIG subcommand")))
}

func info(s *Server, c net.Conn, args []string) (int, error) {
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
			"master_replid:"+s.master.repl_id,
			"master_repl_offset:"+strconv.FormatInt(s.master.repl_offset, 10),
		)
	}

	return c.Write([]byte(resp.EncodeBulkString(strings.Join(infos, "\n"))))
}

func replConf(_ *Server, c net.Conn, args []string) (int, error) {
	if len(args) == 0 {
		return c.Write([]byte(resp.EncodeError("ERR wrong number of arguments for REPLCONFIG subcommand")))
	}

	switch args[0] {
	case "listening-port":
		if len(args) != 2 {
			return c.Write([]byte(resp.EncodeError("ERR wrong number of arguments for REPLCONFIG listening-port subcommand")))
		}
		portStr := args[1]
		log.Println("Replica is listening on port:", portStr)
	case "capa":
		if len(args) < 2 {
			return c.Write([]byte(resp.EncodeError("ERR wrong number of arguments for REPLCONFIG capa subcommand")))
		}
		capaStr := args[1]
		log.Println("Replica supports:", capaStr)
	default:
		return c.Write([]byte(resp.EncodeError("ERR unknown REPLCONFIG subcommand")))
	}

	return c.Write([]byte(resp.EncodeSimpleString(OK)))
}

func psync(s *Server, c net.Conn, args []string) (int, error) {
	if !s.isMaster {
		return c.Write([]byte(resp.EncodeError("Not eligible to serve PSYNC")))
	}
	if len(args) != 2 {
		return c.Write([]byte(resp.EncodeError("ERR wrong number of arguments for PSYNC")))
	}

	n, err := c.Write([]byte(resp.EncodeSimpleString(fmt.Sprintf("%s %s 0", FULLRESYNC, s.master.repl_id))))
	if err != nil {
		return n, err
	}

	// Send empty file
	buf, err := hex.DecodeString(EMPTY_FILE)
	if err != nil {
		return n, err
	}
	fileN, err := c.Write([]byte(resp.EncodeFile(buf)))

	return n + fileN, err
}

func unknown(s *Server, c net.Conn, args []string) (int, error) {
	return c.Write([]byte(resp.EncodeError("ERR unknown command")))
}
