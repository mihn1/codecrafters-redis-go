package resp

import "strings"

type CommandType int

const (
	Ping = iota
	Echo
	Get
	Set
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
	default:
		return Unknown
	}
}
