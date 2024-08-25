package main

import (
	"fmt"
	"strings"

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

	Keys    CommandType = "keys"
	Unknown CommandType = "unknown"
)

type Command struct {
	CommandType CommandType
	Args        []string
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
		Args: make([]string, 0, len(r.Data)-1),
	}

	command.CommandType = CommandType(toLowerString(r.Data[0]))

	for _, b := range r.Data[1:] {
		command.Args = append(command.Args, toLowerString(b))
	}

	return command, nil
}

func ParseCommandFromRawBytes(buffer []byte) (*Command, error) {
	command := &Command{
		Raw: buffer,
	}
	raw := string(buffer)

	tokens, err := resp.ParseArray(strings.TrimSpace(strings.ToLower(raw)))
	if err != nil {
		return command, err
	}

	if len(tokens) == 0 {
		return command, fmt.Errorf("invalid command")
	}

	command.CommandType = CommandType(tokens[0])
	command.Args = tokens[1:]

	return command, nil
}
