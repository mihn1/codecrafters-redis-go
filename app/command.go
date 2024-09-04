package main

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/resp"
)

type CommandType string

const (
	// Core commands
	Ping CommandType = "ping"
	Echo CommandType = "echo"
	Get  CommandType = "get"
	Set  CommandType = "set"

	// Other commands
	Info     CommandType = "info"
	ReplConf CommandType = "replconf"
	Psync    CommandType = "psync"
	Config   CommandType = "config"
	Wait     CommandType = "wait"
	Keys     CommandType = "keys"
	Incr     CommandType = "incr"
	Multi    CommandType = "multi"
	Exec     CommandType = "exec"
	Discard  CommandType = "discard"
	Type     CommandType = "type"
	XAdd     CommandType = "xadd"

	Unknown CommandType = "unknown"
)

type Command struct {
	CommandType CommandType
	Args        [][]byte
	Raw         []byte
	ReplCnt     int32
}

func ParseCommandFromRESP(r resp.RESP) (*Command, error) {
	if r.Type != resp.ARRAY {
		return nil, fmt.Errorf("expect array RESP, getting %v", r.Type)
	}

	if len(r.Data) == 0 {
		return nil, fmt.Errorf("invalid command")
	}

	command := &Command{
		Raw:  r.Raw,
		Args: make([][]byte, 0, len(r.Data)-1),
	}

	command.CommandType = CommandType(ToLowerString(r.Data[0]))
	command.Args = append(command.Args, r.Data[1:]...)
	return command, nil
}
