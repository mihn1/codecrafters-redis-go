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
	QUEUED     = "QUEUED"
	FULLRESYNC = "FULLRESYNC"
	EMPTY_FILE = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)

type commandHandler func(*Server, *Connection, *Command) ([]byte, error)

var commandHandlersMap = map[CommandType]commandHandler{
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
	Incr:     incr,
	Multi:    multi,
	Exec:     exec,
	Discard:  discard,
}

func HandleCommand(s *Server, c *Connection, cmd *Command) error {
	handler, err := resolveHandler(cmd.CommandType)
	if err != nil {
		handler = unknown
		if c.isBatch {
			c.batch.isError = true
		}
	}

	// Queue the command if this is a batch
	if c.isBatch && cmd.CommandType != Exec {
		c.batch.handlerQueue = append(c.batch.handlerQueue, handler)
		_, err := c.conn.Write(resp.EncodeSimpleString(QUEUED))
		return err
	}

	bytes, err := handler(s, c, cmd)

	if err != nil {
		return fmt.Errorf("error handling command: %w", err)
	}

	// Nothing to propagate, the handler has handled everything
	// if len(bytes) == 0 {
	// 	return nil
	// }

	_, err = c.conn.Write(bytes)
	if err == nil {
		maybeReplicateCommand(s, cmd)

		if isFromMaster(s, c) {
			log.Printf("Received %v bytes from master:", len(cmd.Raw))
			// s.mu.Lock() // Don't need to lock cause a connection is handled sequentially
			s.asSlave.offset += int64(len(cmd.Raw)) // TODO: Handle write offset and total offset
			// s.mu.Unlock()
		}
	}
	return err
}

func resolveHandler(cmd CommandType) (commandHandler, error) {
	if f, ok := commandHandlersMap[cmd]; ok {
		return f, nil
	}
	return nil, fmt.Errorf("unknown command type: %v", cmd)
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

func ping(s *Server, c *Connection, cmd *Command) ([]byte, error) {
	if isFromMaster(s, c) {
		return nil, nil // Master connection -> do nothing
	}
	return resp.EncodeSimpleString(PONG), nil
}

func echo(s *Server, c *Connection, cmd *Command) ([]byte, error) {
	if len(cmd.Args) != 1 {
		return resp.EncodeError("wrong number of arguments for 'echo' command"), nil
	}
	return resp.EncodeBulkString(string(cmd.Args[0])), nil
}

func get(s *Server, c *Connection, cmd *Command) ([]byte, error) {
	if len(cmd.Args) < 1 {
		return resp.EncodeError(fmt.Sprintf("wrong number of arguments for GET: %v", len(cmd.Args))), nil
	}

	v, err := s.db.Get(string(cmd.Args[0]))
	if err != nil {
		switch e := err.(type) {
		case *internal.KeyNotFoundError:
			// return resp.EncodeBulkString(""), nil
			return resp.EncodeNullBulkString(), nil
		case *internal.KeyExpiredError:
			return resp.EncodeNullBulkString(), nil
		default:
			return resp.EncodeError(e.Error()), nil
		}
	}

	return resp.EncodeBulkString(string(v.Value)), nil
}

func set(s *Server, c *Connection, cmd *Command) ([]byte, error) {
	if len(cmd.Args) != 2 && len(cmd.Args) != 4 {
		return resp.EncodeError(fmt.Sprintf("wrong number of arguments for SET: %v", len(cmd.Args))), nil
	}

	err := setInternal(s, cmd)
	if isFromMaster(s, c) {
		// No need to respond to the master
		return nil, err
	}

	if err != nil {
		return resp.EncodeError(err.Error()), nil
	}
	return resp.EncodeSimpleString(OK), nil
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

func config(s *Server, c *Connection, cmd *Command) ([]byte, error) {
	if len(cmd.Args) == 0 {
		return resp.EncodeError("wrong number of arguments for CONFIG commands"), nil
	}

	subCmd := ToLowerString(cmd.Args[0])
	if subCmd == "get" {
		if len(cmd.Args) != 2 {
			return resp.EncodeError("wrong number of arguments for CONFIG GET"), nil
		}

		subCmd1 := ToLowerString(cmd.Args[1])
		switch subCmd1 {
		case "dir":
			return resp.EncodeArrayBulkStrings([]string{"dir", s.db.Options.Dir}), nil
		case "dbfilename":
			return resp.EncodeArrayBulkStrings([]string{"dbfilename", s.db.Options.DbFilename}), nil
		default:
			return resp.EncodeError("unknown CONFIG parameter"), nil
		}
	}

	return resp.EncodeError("unknown CONFIG subcommand"), nil
}

func wait(s *Server, c *Connection, cmd *Command) ([]byte, error) {
	if !s.isMaster {
		return resp.EncodeError("only available in master mode"), nil
	}

	if len(cmd.Args) != 2 {
		return resp.EncodeError("wrong number of arguments for WAIT"), nil
	}

	numRepls, err := strconv.Atoi(string(cmd.Args[0]))
	if err != nil {
		return resp.EncodeError("Innalid numreplicas of arguments for WAIT"), nil
	}

	timeout, err := strconv.Atoi(string(cmd.Args[1]))
	if err != nil {
		return resp.EncodeError("Innalid timeout of arguments for WAIT"), nil
	}
	log.Println("numRepls:", numRepls, "timeout:", timeout)
	cnt := countReplicasAcked(s.asMaster, numRepls, timeout)
	return resp.EncodeInterger(int64(cnt)), nil
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

func info(s *Server, c *Connection, cmd *Command) ([]byte, error) {
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

	return resp.EncodeBulkString(strings.Join(infos, "\n")), nil
}

func replConf(s *Server, c *Connection, cmd *Command) ([]byte, error) {
	if len(cmd.Args) == 0 {
		return resp.EncodeError("wrong number of arguments for REPLCONFIG subcommand"), nil
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
			return resp.EncodeError("Not eligible to serve REPLCONF"), nil
		}
		if len(cmd.Args) != 2 {
			return resp.EncodeError("wrong number of arguments for REPLCONFIG listening-port subcommand"), nil
		}
		portStr := string(cmd.Args[1])
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return resp.EncodeError("invalid listening port"), nil
		}
		s.asMaster.slaves[c.id].listeningPort = port
		log.Println("Replica is listening on port:", portStr)
	case "capa":
		if !s.isMaster {
			return resp.EncodeError("Not eligible to serve REPLCONF"), nil
		}
		if len(cmd.Args) < 2 {
			return resp.EncodeError("wrong number of arguments for REPLCONFIG capa subcommand"), nil
		}
		capaStr := string(cmd.Args[1])
		s.asMaster.slaves[c.id].capa = append(s.asMaster.slaves[c.id].capa, capaStr)
		log.Println("Replica supports:", capaStr)
	case "getack":
		if s.isMaster {
			return resp.EncodeError("Only slave can serve REPLCONF getack"), nil
		}
		if len(cmd.Args) < 2 {
			return resp.EncodeError("wrong number of arguments for REPLCONFIG getack subcommand"), nil
		}
		// ignore the rest of the cmd.Agrs for now
		resArr := []string{"REPLCONF", "ACK", strconv.FormatInt(s.asSlave.offset, 10)}
		return resp.EncodeArrayBulkStrings(resArr), nil
	case "ack":
		if !s.isMaster {
			return resp.EncodeError("Not eligible to serve REPLCONF ACK"), nil
		}
		if len(cmd.Args) < 2 {
			return resp.EncodeError("wrong number of arguments for REPLCONFIG ACK subcommand"), nil
		}
		offset, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
		if err != nil {
			return resp.EncodeError("invalid offset"), nil
		}
		s.asMaster.slaves[c.id].ackOffset = offset
		log.Println("Replica ACKed offset:", offset)
		return nil, nil
	default:
		return resp.EncodeError("unknown REPLCONFIG subcommand"), nil
	}

	return resp.EncodeSimpleString(OK), nil
}

func psync(s *Server, c *Connection, cmd *Command) ([]byte, error) {
	if !s.isMaster {
		return resp.EncodeError("Not eligible to serve PSYNC"), nil
	}
	if len(cmd.Args) != 2 {
		return resp.EncodeError("wrong number of arguments for PSYNC"), nil
	}

	if _, ok := s.asMaster.slaves[c.id]; !ok {
		return nil, fmt.Errorf("slave not found")
	}

	_, err := c.conn.Write(resp.EncodeSimpleString(fmt.Sprintf("%s %s 0", FULLRESYNC, s.asMaster.repl_id)))
	if err != nil {
		return nil, err
	}

	// Send empty file
	buf, err := hex.DecodeString(EMPTY_FILE)
	if err != nil {
		return nil, err
	}
	_, err = c.conn.Write(resp.EncodeFile(buf))

	return nil, err
}

func keys(s *Server, c *Connection, cmd *Command) ([]byte, error) {
	if len(cmd.Args) != 1 {
		return resp.EncodeError("wrong number of arguments for 'KEYS' command"), nil
	}

	pattern := cmd.Args[0]
	log.Println("Pattern for matching keys:", pattern)
	filePath := filepath.Join(s.db.Options.Dir, s.db.Options.DbFilename)
	rdbReader := internal.NewRDBReader()

	// TODO: implement read keys from rdbReader instead of load the whole file
	data, err := rdbReader.LoadFile(filePath)
	if err != nil {
		return resp.EncodeError(fmt.Sprintf("can't load rdb file at %s", filePath)), nil
	}

	keys := make([]string, len(data))
	i := 0
	for key := range data {
		keys[i] = key
		i++
	}

	return resp.EncodeArrayBulkStrings(keys), nil
}

func incr(s *Server, c *Connection, cmd *Command) ([]byte, error) {
	if len(cmd.Args) != 1 {
		return resp.EncodeError("wrong number of arguments for 'incr' commands"), nil
	}
	key := string(cmd.Args[0])
	val, err := s.db.Get(key)
	if err != nil {
		s.db.Set(key, []byte("1"), 0)
		return resp.EncodeInterger(1), nil
	}

	valStr := string(val.Value)
	valInt, err := strconv.Atoi(valStr)
	if err != nil {
		return resp.EncodeError("value is not an integer or out of range"), nil
	}

	valInt++
	s.db.Set(key, []byte(strconv.Itoa(valInt)), val.ExpiredTimeMilli)
	return resp.EncodeInterger(int64(valInt)), nil
}

func multi(_ *Server, c *Connection, cmd *Command) ([]byte, error) {
	if len(cmd.Args) != 0 {
		return resp.EncodeError("wrong number of arguments for 'multi' command"), nil
	}

	if c.isBatch {
		return resp.EncodeError("MULTI calls can not be nested"), nil
	}

	c.isBatch = true
	c.batch = &Batch{
		isError:      false,
		handlerQueue: make([]commandHandler, 0),
	}

	return resp.EncodeSimpleString(OK), nil
}

func exec(s *Server, c *Connection, cmd *Command) ([]byte, error) {
	if len(cmd.Args) != 0 {
		return resp.EncodeError("wrong number of arguments for 'exec' command"), nil
	}

	if !c.isBatch {
		return resp.EncodeError("EXEC without MULTI"), nil
	}

	if c.batch.isError {
		return resp.EncodeErrorNoPrefix("EXEC aborted due to previous errors"), nil
	}

	resArray := make([][]byte, 0, len(c.batch.handlerQueue))
	for _, handler := range c.batch.handlerQueue {
		handledBytes, err := handler(s, c, cmd)
		if err != nil {
			// Continue the execution even if a handler fails
			c.batch.isError = true
		}
		if len(handledBytes) > 0 {
			resArray = append(resArray, handledBytes)
		}
	}
	// reset the connection
	c.isBatch = false
	c.batch = nil
	return resp.EncodeArray(resArray), nil
}

func discard(s *Server, c *Connection, cmd *Command) ([]byte, error) {
	if len(cmd.Args) != 0 {
		return resp.EncodeError("wrong number of arguments for 'discard' command"), nil
	}

	if !c.isBatch {
		return resp.EncodeError("DISCARD without MULTI"), nil
	}

	c.isBatch = false
	c.batch = nil
	return resp.EncodeSimpleString(OK), nil
}

func unknown(s *Server, c *Connection, cmd *Command) ([]byte, error) {
	return resp.EncodeError("unknown command"), nil
}
