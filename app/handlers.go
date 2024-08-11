package main

import (
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
)

var commandHandlers = map[CommandType]func(*Server, []string) string{
	Ping:     ping,
	Echo:     echo,
	Set:      set,
	Get:      get,
	Info:     info,
	Config:   config,
	ReplConf: replConf,
	Psync:    psync,
}

func HandleCommand(s *Server, command Command) string {
	handler := resolveHandler(command.CommandType)
	return handler(s, command.Agrs)
}

func resolveHandler(command CommandType) func(*Server, []string) string {
	if f, ok := commandHandlers[command]; ok {
		return f
	}
	return unknown
}

func ping(s *Server, args []string) string {
	return resp.EncodeSimpleString("PONG")
}

func echo(s *Server, args []string) string {
	return resp.EncodeBulkString(args[0])
}

func get(s *Server, args []string) string {
	if len(args) < 1 {
		return resp.EncodeError(fmt.Sprintf("ERR wrong number of arguments for GET: %v", len(args)))
	}

	v, err := s.db.Get(args[0])
	if err != nil {
		switch e := err.(type) {
		case *internal.KeyNotFoundError:
			return resp.EncodeBulkString("")
		case *internal.KeyExpiredError:
			return resp.NULL_BULK_STRING
		default:
			return resp.EncodeError(e.Error())
		}
	}

	return resp.EncodeBulkString(v.Value)
}

func set(s *Server, args []string) string {
	if len(args) != 2 && len(args) != 4 {
		return resp.EncodeError(fmt.Sprintf("ERR wrong number of arguments for SET: %v", len(args)))
	}

	key := args[0]
	val := args[1]
	var expiryMilis int64 = s.db.Options.ExpiryTime

	// resolve expiry
	if len(args) == 4 {
		expiryType := args[2]
		expiryNum, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return resp.EncodeError(err.Error())
		}

		expiryNum, err = resolveExpiry(expiryType, expiryNum)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		expiryMilis = expiryNum
	}

	s.db.Set(key, val, expiryMilis)
	return resp.EncodeSimpleString(OK)
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

func config(s *Server, args []string) string {
	if len(args) == 0 {
		return resp.EncodeError("ERR wrong number of arguments for CONFIG commands")
	}

	if args[0] == "get" {
		if len(args) != 2 {
			return resp.EncodeError("ERR wrong number of arguments for CONFIG GET")
		}

		switch args[1] {
		case "dir":
			return resp.EncodeArrayBulkStrings([]string{"dir", s.db.Options.Dir})
		case "s.dbfilename":
			return resp.EncodeArrayBulkStrings([]string{"s.dbfilename", s.db.Options.DbFilename})
		default:
			return resp.EncodeError("ERR unknown CONFIG parameter")
		}
	}

	return resp.EncodeError("ERR unknown CONFIG subcommand")
}

func info(s *Server, args []string) string {
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

	return resp.EncodeBulkString(strings.Join(infos, "\n"))
}

func replConf(_ *Server, args []string) string {
	if len(args) == 0 {
		return resp.EncodeError("ERR wrong number of arguments for REPLCONFIG subcommand")
	}

	switch args[0] {
	case "listening-port":
		if len(args) != 2 {
			return resp.EncodeError("ERR wrong number of arguments for REPLCONFIG listening-port subcommand")
		}
		portStr := args[1]
		log.Println("Replica is listening on port:", portStr)
	case "capa":
		if len(args) < 2 {
			return resp.EncodeError("ERR wrong number of arguments for REPLCONFIG capa subcommand")
		}
		capaStr := args[1]
		log.Println("Replica supports:", capaStr)
	default:
		return resp.EncodeError("ERR unknown REPLCONFIG subcommand")
	}

	return resp.EncodeSimpleString(OK)
}

func psync(s *Server, args []string) string {
	if !s.isMaster {
		return resp.EncodeError("Not eligible to serve PSYNC")
	}
	if len(args) != 2 {
		return resp.EncodeError("ERR wrong number of arguments for PSYNC")
	}

	return resp.EncodeSimpleString(fmt.Sprintf("%s %s 0", FULLRESYNC, s.master.repl_id))
}

func unknown(s *Server, args []string) string {
	return resp.EncodeError("ERR unknown command")
}
