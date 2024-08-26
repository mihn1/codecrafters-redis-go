package main

import (
	"bufio"
	"fmt"
	"strings"
	"testing"

	"github.com/codecrafters-io/redis-starter-go/resp"
	"github.com/stretchr/testify/assert"
)

func TestParseEchoCommand(t *testing.T) {
	// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n -> ["ECHO", "hey"]

	raw := "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
	reader := bufio.NewReader(strings.NewReader(raw))
	rp, err := resp.ReadNextResp(reader)
	if err != nil {
		t.Fatal(err)
	}
	command, err := ParseCommandFromRESP(rp)
	if err != nil {
		t.Fatal(err)
	}

	assert.EqualValues(t, Echo, command.CommandType)
	assert.Equal(t, [][]byte{[]byte("hey")}, command.Args)
}

func TestParseREPLCONF_CAPACommand(t *testing.T) {
	// *3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n -> ["REPLCONF", "capa", "psync2"]

	raw := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	reader := bufio.NewReader(strings.NewReader(raw))
	rp, err := resp.ReadNextResp(reader)
	if err != nil {
		t.Fatal(err)
	}
	command, err := ParseCommandFromRESP(rp)
	if err != nil {
		t.Fatal(err)
	}

	assert.EqualValues(t, ReplConf, command.CommandType)
	assert.Equal(t, [][]byte{[]byte("capa"), []byte("psync2")}, command.Args)
}

func TestParseEchoCommandFromRESP(t *testing.T) {
	raw := "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
	reader := bufio.NewReader(strings.NewReader(raw))
	r, err := resp.ReadNextResp(reader)
	if err != nil {
		fmt.Println("Error reading next RESP", err)
		t.Fatal(err)
	}

	command, err := ParseCommandFromRESP(r)
	if err != nil {
		fmt.Println("Error parsing command from RESP", err)
		t.Fatal(err)
	}

	fmt.Println("Command: ", *command)
	fmt.Println("raw string", string(command.Raw))
	assert.EqualValues(t, Echo, command.CommandType)
}

func TestParseSetCommandFromRESP(t *testing.T) {
	// raw := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n"
	raw := "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
	reader := bufio.NewReader(strings.NewReader(raw))
	r, err := resp.ReadNextResp(reader)
	if err != nil {
		fmt.Println("Error reading next RESP", err)
		t.Fatal(err)
	}

	command, err := ParseCommandFromRESP(r)
	if err != nil {
		fmt.Println("Error parsing command from RESP", err)
		t.Fatal(err)
	}

	fmt.Println("Command: ", *command)
	fmt.Println("raw string", string(command.Raw))
	assert.EqualValues(t, Set, command.CommandType)
}
