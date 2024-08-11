package main

import (
	"strings"

	"github.com/codecrafters-io/redis-starter-go/resp"
)

type CommandType int

const (
	// Core commands
	Ping = iota
	Echo
	Get
	Set

	// Other commands
	Info
	ReplConf
	Config

	Unknown
)

func resolveCommandType(raw string) CommandType {
	raw = strings.ToLower(raw)
	switch raw {
	case "ping":
		return Ping
	case "echo":
		return Echo
	case "get":
		return Get
	case "set":
		return Set

	case "info":
		return Info
	case "replconf":
		return ReplConf
	case "config":
		return Config
	default:
		return Unknown
	}
}

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

	commandType := resolveCommandType(tokens[0])
	command.CommandType = commandType
	command.Agrs = tokens[1:]

	return command, nil
}
