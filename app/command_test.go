package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseEchoCommand(t *testing.T) {
	// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n -> ["ECHO", "hey"]

	raw := "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
	command, err := ParseCommand(raw)
	if err != nil {
		t.Fatal(err)
	}

	assert.EqualValues(t, Echo, command.CommandType)
	assert.Equal(t, []string{"hey"}, command.Agrs)
}
