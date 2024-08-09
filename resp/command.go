package resp

import (
	"fmt"
	"strconv"
	"strings"
)

type Command struct {
	CommandType CommandType
	Agrs        []string
}

func ParseCommand(raw string) (Command, error) {
	var command Command

	tokens, err := parseArray(strings.TrimSpace(raw))
	if err != nil {
		return command, err
	}

	commandType := resolveCommandType(tokens[0])
	command.CommandType = commandType
	command.Agrs = tokens[1:]

	return command, nil
}

func parseArray(raw string) ([]string, error) {
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
