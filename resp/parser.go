package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type RespType byte

const (
	SIMPLE_STRING RespType = '+'
	ERROR         RespType = '-'
	INTEGER       RespType = ':'
	BULK_STRING   RespType = '$'
	ARRAY         RespType = '*'

	CR byte = '\r'
	LF byte = '\n'
)

type RESP struct {
	Type RespType
	Data [][]byte
	Raw  []byte
}

func ReadNextResp(reader *bufio.Reader) (RESP, error) {
	resp := RESP{}

	line, err := ReadLine(reader)
	if err != nil {
		return resp, err
	}
	if len(line) == 0 {
		return resp, nil // no data to read
	}

	resp.Type = RespType(line[0])
	resp.Raw = line
	switch resp.Type {
	case SIMPLE_STRING, ERROR, INTEGER:
		resp.Data = [][]byte{line[1 : len(line)-2]} // trim the first byte and the last \r\n
		return resp, nil
	case BULK_STRING:
		size, err := strconv.Atoi(string(line[1 : len(line)-2]))
		if err != nil {
			return resp, err
		}

		bulkStrLine, err := ReadLine(reader)
		if err != nil {
			return resp, err
		}
		if len(bulkStrLine)-2 != size {
			return resp, fmt.Errorf("invalid bulk string length")
		}
		resp.Raw = append(resp.Raw, bulkStrLine...)
		resp.Data = [][]byte{bulkStrLine[:len(bulkStrLine)-2]}
		return resp, nil
	case ARRAY:
		size, err := strconv.Atoi(string(line[1 : len(line)-2]))
		if err != nil {
			return resp, err
		}

		resp.Data = make([][]byte, 0, size)
		for i := 0; i < size; i++ {
			nxtResp, err := ReadNextResp(reader)
			if err != nil {
				return resp, err
			}

			resp.Raw = append(resp.Raw, nxtResp.Raw...)
			resp.Data = append(resp.Data, nxtResp.Data...)
		}
		return resp, nil
	default:
		return resp, fmt.Errorf("invalid RESP type")
	}
}

func ReadLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes(LF)
	if err != nil && err != io.EOF {
		return nil, err
	}

	if err == io.EOF && len(line) == 0 {
		return nil, io.EOF
	}

	if len(line) == 0 {
		return []byte{}, nil
	}

	if line[len(line)-2] != CR {
		return nil, fmt.Errorf("invalid RESP line")
	}

	return line, nil
}

func ParseArray(raw string) ([]string, error) {
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

func ParseSimpleString(raw string) (string, error) {
	if len(raw) < 2 {
		return "", fmt.Errorf("invalid simple string length")
	}

	return raw[1 : len(raw)-2], nil
}

func ParseBulkString(sizeIdentifier string, raw string) (string, error) {
	if len(sizeIdentifier) < 2 {
		return "", fmt.Errorf("invalid bulk string size identifier")
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

		token, err = ParseBulkString(identifier, tokens[1])
		if err == nil {
			tokens = tokens[2:]
		}
	default:
		return token, tokens, fmt.Errorf("invalid token")
	}

	return token, tokens, err
}
