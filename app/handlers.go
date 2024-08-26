package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal"
	"github.com/codecrafters-io/redis-starter-go/resp"
)

const (
	OK         = "OK"
	PONG       = "PONG"
	FULLRESYNC = "FULLRESYNC"
	EMPTY_FILE = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)

var commandHandlers = map[CommandType]func(*Server, *Connection, *Command) (int, error){
	Ping:     ping,
	Echo:     echo,
	Set:      set,
	Get:      get,
	Info:     info,
	Wait:     wait,
	Config:   config,
	ReplConf: replConf,
	Psync:    psync,
	Keys:     keys,
	Incr:    incr,
}

func HandleCommand(s *Server, c *Connection, cmd *Command) (int, error) {
	handler := resolveHandler(cmd.CommandType)
	n, err := handler(s, c, cmd)
	if err == nil {
		maybeReplicateCommand(s, cmd)

		if isFromMaster(s, c) {
			log.Printf("Received %v bytes from master:", len(cmd.Raw))
			// s.mu.Lock() // Don't need to lock cause a connection is handled sequentially
			s.asSlave.offset += int64(len(cmd.Raw)) // TODO: Handle write offset and total offset
			// s.mu.Unlock()
		}
	}
	return n, err
}

func resolveHandler(cmd CommandType) func(*Server, *Connection, *Command) (int, error) {
	if f, ok := commandHandlers[cmd]; ok {
		return f
	}
	return unknown
}

func maybeReplicateCommand(s *Server, cmd *Command) {
	if cmd.CommandType == Set && s.isMaster {
		// FOR NOW: master's repl offset increases with each write command
		s.mu.Lock()
		s.asMaster.repl_offset += int64(len(cmd.Raw))
		s.mu.Unlock()

		if len(s.asMaster.slaves) > 0 {
			log.Println("Replicating command to", len(s.asMaster.slaves), "slaves")
			for _, slave := range s.asMaster.slaves {
				go replicate(slave, cmd)
			}
		}
	}
}

func replicate(slave *Slave, command *Command) (int, error) {
	n, err := slave.connection.conn.Write(command.Raw)
	if err != nil {
		log.Printf("Error replicating %v to %v: %v\n", command.CommandType, slave.connection.conn.RemoteAddr(), err)
	} else {
		atomic.AddInt32(&command.ReplCnt, 1)
		atomic.AddInt64(&slave.syncOffset, int64(n))
	}
	return n, nil
}

func isFromMaster(s *Server, c *Connection) bool {
	return !s.isMaster && s.asSlave.masterConnection.id == c.id
}

func ping(s *Server, c *Connection, cmd *Command) (int, error) {
	if isFromMaster(s, c) {
		return 0, nil // Master connection -> do nothing
	}
	return c.conn.Write(resp.EncodeSimpleString(PONG))
}

func echo(s *Server, c *Connection, cmd *Command) (int, error) {
	if len(cmd.Args) != 1 {
		return c.conn.Write(resp.EncodeError("wrong number of arguments for 'echo' command"))
	}
	return c.conn.Write(resp.EncodeBulkString(string(cmd.Args[0])))
}

func get(s *Server, c *Connection, cmd *Command) (int, error) {
	if len(cmd.Args) < 1 {
		return c.conn.Write(resp.EncodeError(fmt.Sprintf("wrong number of arguments for GET: %v", len(cmd.Args))))
	}

	v, err := s.db.Get(string(cmd.Args[0]))
	if err != nil {
		switch e := err.(type) {
		case *internal.KeyNotFoundError:
			return c.conn.Write(resp.EncodeBulkString(""))
		case *internal.KeyExpiredError:
			return c.conn.Write(resp.EncodeNullBulkString())
		default:
			return c.conn.Write(resp.EncodeError(e.Error()))
		}
	}

	return c.conn.Write(resp.EncodeBulkString(string(v.Value)))
}

func set(s *Server, c *Connection, cmd *Command) (int, error) {
	if len(cmd.Args) != 2 && len(cmd.Args) != 4 {
		return c.conn.Write(resp.EncodeError(fmt.Sprintf("wrong number of arguments for SET: %v", len(cmd.Args))))
	}

	err := setInternal(s, cmd)
	if isFromMaster(s, c) {
		// No need to respond to the master
		return 0, err
	}

	if err != nil {
		return c.conn.Write(resp.EncodeError(err.Error()))
	}
	return c.conn.Write(resp.EncodeSimpleString(OK))
}

func setInternal(s *Server, cmd *Command) error {
	key := string(cmd.Args[0])
	val := cmd.Args[1]
	var expiryMilis int64 = s.db.Options.ExpiryTime

	// resolve expiry
	if len(cmd.Args) == 4 {
		expiryType := ToLowerString(cmd.Args[2])
		expiryNum, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
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

func config(s *Server, c *Connection, cmd *Command) (int, error) {
	if len(cmd.Args) == 0 {
		return c.conn.Write(resp.EncodeError("wrong number of arguments for CONFIG commands"))
	}

	subCmd := ToLowerString(cmd.Args[0])
	if subCmd == "get" {
		if len(cmd.Args) != 2 {
			return c.conn.Write(resp.EncodeError("wrong number of arguments for CONFIG GET"))
		}

		subCmd1 := ToLowerString(cmd.Args[1])
		switch subCmd1 {
		case "dir":
			return c.conn.Write(resp.EncodeArrayBulkStrings([]string{"dir", s.db.Options.Dir}))
		case "dbfilename":
			return c.conn.Write(resp.EncodeArrayBulkStrings([]string{"dbfilename", s.db.Options.DbFilename}))
		default:
			return c.conn.Write(resp.EncodeError("unknown CONFIG parameter"))
		}
	}

	return c.conn.Write(resp.EncodeError("unknown CONFIG subcommand"))
}

func wait(s *Server, c *Connection, cmd *Command) (int, error) {
	if !s.isMaster {
		return c.conn.Write(resp.EncodeError("only available in master mode"))
	}

	if len(cmd.Args) != 2 {
		return c.conn.Write(resp.EncodeError("wrong number of arguments for WAIT"))
	}

	numRepls, err := strconv.Atoi(string(cmd.Args[0]))
	if err != nil {
		return c.conn.Write(resp.EncodeError("Innalid numreplicas of arguments for WAIT"))
	}

	timeout, err := strconv.Atoi(string(cmd.Args[1]))
	if err != nil {
		return c.conn.Write(resp.EncodeError("Innalid timeout of arguments for WAIT"))
	}
	log.Println("numRepls:", numRepls, "timeout:", timeout)
	cnt := countReplicasAcked(s.asMaster, numRepls, timeout)
	return c.conn.Write(resp.EncodeInterger(int64(cnt)))
}

func countReplicasAcked(m AsMasterInfo, numRepls int, timeoutMilis int) int {
	repl_offset := m.repl_offset
	count := 0 // master itself
	sentGetAcks := false
	timer := time.NewTimer(time.Duration(timeoutMilis) * time.Millisecond)
	for {
		select {
		case <-timer.C:
			return count
		default:
			tmp := 0
			for _, slave := range m.slaves {
				// log.Println("slave", slave.connection.id, "ackOffset:", slave.ackOffset, "repl_offset:", repl_offset)
				if slave.ackOffset >= repl_offset {
					tmp += 1
					if tmp > count {
						count = tmp
					}
				} else if !sentGetAcks {
					go sendGETACK(slave)
				}
			}
			sentGetAcks = true // Just send 1 round of GETACK
			if count >= numRepls {
				timer.Stop()
				return count
			}
			time.Sleep(time.Duration(100) * time.Millisecond) // avoid CPU overload
		}
	}
}

func info(s *Server, c *Connection, cmd *Command) (int, error) {
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

	if len(cmd.Args) > 0 {
		// TODO: add more info
		log.Println("TODO: add more info for cmd.Agrs:", cmd.Args)
	}

	if s.isMaster {
		infos = append(infos,
			"master_replid:"+s.asMaster.repl_id,
			"master_repl_offset:"+strconv.FormatInt(s.asMaster.repl_offset, 10),
		)
	}

	return c.conn.Write(resp.EncodeBulkString(strings.Join(infos, "\n")))
}

func replConf(s *Server, c *Connection, cmd *Command) (int, error) {
	if len(cmd.Args) == 0 {
		return c.conn.Write(resp.EncodeError("wrong number of arguments for REPLCONFIG subcommand"))
	}

	// TODO: move this logic to be handled by the master struct
	if s.isMaster {
		var slave *Slave
		s.mu.Lock()
		if _, ok := s.asMaster.slaves[c.id]; !ok {
			slave = &Slave{
				connection: c,
				capa:       make([]string, 0),
			}
			s.asMaster.slaves[c.id] = slave
		}
		s.mu.Unlock()
	}

	subCmd := ToLowerString(cmd.Args[0])
	switch subCmd {
	case "listening-port":
		if !s.isMaster {
			return c.conn.Write(resp.EncodeError("Not eligible to serve REPLCONF"))
		}
		if len(cmd.Args) != 2 {
			return c.conn.Write(resp.EncodeError("wrong number of arguments for REPLCONFIG listening-port subcommand"))
		}
		portStr := string(cmd.Args[1])
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return c.conn.Write(resp.EncodeError("invalid listening port"))
		}
		s.asMaster.slaves[c.id].listeningPort = port
		log.Println("Replica is listening on port:", portStr)
	case "capa":
		if !s.isMaster {
			return c.conn.Write(resp.EncodeError("Not eligible to serve REPLCONF"))
		}
		if len(cmd.Args) < 2 {
			return c.conn.Write(resp.EncodeError("wrong number of arguments for REPLCONFIG capa subcommand"))
		}
		capaStr := string(cmd.Args[1])
		s.asMaster.slaves[c.id].capa = append(s.asMaster.slaves[c.id].capa, capaStr)
		log.Println("Replica supports:", capaStr)
	case "getack":
		if s.isMaster {
			return c.conn.Write(resp.EncodeError("Only slave can serve REPLCONF getack"))
		}
		if len(cmd.Args) < 2 {
			return c.conn.Write(resp.EncodeError("wrong number of arguments for REPLCONFIG getack subcommand"))
		}
		// ignore the rest of the cmd.Agrs for now
		resArr := []string{"REPLCONF", "ACK", strconv.FormatInt(s.asSlave.offset, 10)}
		return c.conn.Write(resp.EncodeArrayBulkStrings(resArr))
	case "ack":
		if !s.isMaster {
			return c.conn.Write(resp.EncodeError("Not eligible to serve REPLCONF ACK"))
		}
		if len(cmd.Args) < 2 {
			return c.conn.Write(resp.EncodeError("wrong number of arguments for REPLCONFIG ACK subcommand"))
		}
		offset, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
		if err != nil {
			return c.conn.Write(resp.EncodeError("invalid offset"))
		}
		s.asMaster.slaves[c.id].ackOffset = offset
		log.Println("Replica ACKed offset:", offset)
		return 0, nil
	default:
		return c.conn.Write(resp.EncodeError("unknown REPLCONFIG subcommand"))
	}

	return c.conn.Write(resp.EncodeSimpleString(OK))
}

func psync(s *Server, c *Connection, cmd *Command) (int, error) {
	if !s.isMaster {
		return c.conn.Write(resp.EncodeError("Not eligible to serve PSYNC"))
	}
	if len(cmd.Args) != 2 {
		return c.conn.Write(resp.EncodeError("wrong number of arguments for PSYNC"))
	}

	if _, ok := s.asMaster.slaves[c.id]; !ok {
		return 0, fmt.Errorf("slave not found")
	}

	n, err := c.conn.Write(resp.EncodeSimpleString(fmt.Sprintf("%s %s 0", FULLRESYNC, s.asMaster.repl_id)))
	if err != nil {
		return n, err
	}

	// Send empty file
	buf, err := hex.DecodeString(EMPTY_FILE)
	if err != nil {
		return n, err
	}
	fileN, err := c.conn.Write(resp.EncodeFile(buf))

	return n + fileN, err
}

func keys(s *Server, c *Connection, cmd *Command) (int, error) {
	if len(cmd.Args) != 1 {
		return c.conn.Write(resp.EncodeError("wrong number of arguments for 'KEYS' command"))
	}

	pattern := cmd.Args[0]
	log.Println("Pattern for matching keys:", pattern)
	filePath := filepath.Join(s.db.Options.Dir, s.db.Options.DbFilename)
	rdbReader := internal.NewRDBReader()

	// TODO: implement read keys from rdbReader instead of load the whole file
	data, err := rdbReader.LoadFile(filePath)
	if err != nil {
		return c.conn.Write(resp.EncodeError(fmt.Sprintf("can't load rdb file at %s", filePath)))
	}

	keys := make([]string, len(data))
	i := 0
	for key := range data {
		keys[i] = key
		i++
	}

	return c.conn.Write(resp.EncodeArrayBulkStrings(keys))
}

func incr(s *Server, c *Connection, cmd *Command) (int, error) {
	if len(cmd.Args) != 1 {
		return c.conn.Write(resp.EncodeError("wrong number of arguments for 'incr' commands"))
	}
	key := string(cmd.Args[0])
	val, err := s.db.Get(key)
	if err != nil {
		s.db.Set(key, []byte("1"), 0)
		return c.conn.Write(resp.EncodeInterger(1))
	}

	valStr := string(val.Value)
	valInt, err := strconv.Atoi(valStr)
	if err != nil {
		c.conn.Write(resp.EncodeError("value is not an integer or out of range"))
	}

	valInt++
	s.db.Set(key, []byte(strconv.Itoa(valInt)), val.ExpiredTimeMilli)
	return c.conn.Write(resp.EncodeInterger(int64(valInt)))
}

func unknown(s *Server, c *Connection, cmd *Command) (int, error) {
	return c.conn.Write(resp.EncodeError("unknown command"))
}
