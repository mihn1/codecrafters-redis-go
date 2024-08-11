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

	Unknown CommandType = "unknown"
)

type Command struct {
	CommandType CommandType
	Agrs        []string
}

func ParseCommand(raw string) (Command, error) {
	var command Command

	tokens, err := resp.ParseArray(strings.TrimSpace(strings.ToLower(raw)))
	if err != nil {
		return command, err
	}

	if len(tokens) == 0 {
		return command, fmt.Errorf("invalid command")
	}

	command.CommandType = CommandType(tokens[0])
	command.Agrs = tokens[1:]

	return command, nil
}
