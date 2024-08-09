package utils

import (
	"fmt"
	"strconv"
	"strings"
)

type CommandType int

const (
	Ping = iota
	Echo
	Unknown
)

type Command struct {
	commandType CommandType
	agrs        []string
}

var commandHandlers = map[CommandType]func([]string) string{
	Ping:    ping,
	Echo:    echo,
	Unknown: unknown,
}

func ping(args []string) string {
	return "+PONG\r\n"
}

func echo(args []string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0])
}

func unknown(args []string) string {
	return fmt.Sprintf("-ERR unknown command '%s'\r\n", args[0])
}

func HandleCommand(command Command) string {
	return commandHandlers[command.commandType](command.agrs)
}

func ParseCommand(raw string) (Command, error) {
	var command Command

	tokens, err := parseArray(strings.TrimSpace(raw))
	if err != nil {
		return command, err
	}

	commandType := parseCommandType(tokens[0])
	command.commandType = commandType
	command.agrs = tokens[1:]

	return command, nil
}

func parseArray(raw string) ([]string, error) {
	// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n -> ["ECHO", "hey"]
	var arr []string
	rawTokens := strings.Split(raw, "\r\n")
	for len(rawTokens) > 0 {
		// parse the first token *2
		if cap(arr) == 0 {
			sizeIdentifier := rawTokens[0]
			if len(sizeIdentifier) < 2 {
				return arr, fmt.Errorf("invalid array length")
			}

			le, err := strconv.Atoi(sizeIdentifier[1:])
			if err != nil {
				return arr, err
			}

			arr = make([]string, 0, le)
			rawTokens = rawTokens[1:]
			continue
		}

		// add other tokens
		token, nxtTokens, err := resolveNextToken(rawTokens)
		if err != nil {
			return arr, err
		}

		rawTokens = nxtTokens
		arr = append(arr, token)
	}

	return arr, nil
}

func resolveNextToken(tokens []string) (string, []string, error) {
	// token is ensured not empty here
	var token string
	var err error
	identifier := tokens[0]
	firstByte := identifier[0]

	switch firstByte {
	case '$': // bulk string
		if len(tokens) < 2 {
			return token, tokens, fmt.Errorf("invalid bulk string")
		}

		token, err = parseBulkString(identifier, tokens[1])
		if err == nil {
			tokens = tokens[2:]
		}
	default:
		return token, tokens, fmt.Errorf("invalid token")
	}

	return token, tokens, err
}

func parseBulkString(sizeIdentifier string, raw string) (string, error) {
	if len(sizeIdentifier) < 2 {
		return "", nil
	}

	le, err := strconv.Atoi(sizeIdentifier[1:])
	if err != nil {
		return "", err
	}

	if le != len(raw) {
		return "", fmt.Errorf("invalid bulk string length")
	}

	return raw, nil
}

func parseCommandType(raw string) CommandType {
	raw = strings.ToLower(raw)
	switch raw {
	case "ping":
		return Ping
	case "echo":
		return Echo
	default:
		return Unknown
	}
}
