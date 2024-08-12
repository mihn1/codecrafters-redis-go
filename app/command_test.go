package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseEchoCommand(t *testing.T) {
	// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n -> ["ECHO", "hey"]

	raw := "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
	command, err := ParseCommand([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}

	assert.EqualValues(t, Echo, command.CommandType)
	assert.Equal(t, []string{"hey"}, command.Agrs)
}

func TestParseREPLCONF_CAPACommand(t *testing.T) {
	// *3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n -> ["REPLCONF", "capa", "psync2"]

	raw := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	command, err := ParseCommand([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}

	assert.EqualValues(t, ReplConf, command.CommandType)
	assert.Equal(t, []string{"hey"}, command.Agrs)
}
