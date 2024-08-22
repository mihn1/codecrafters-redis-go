package internal

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecodeSize00(t *testing.T) {
	input := []byte{0x0A}
	reader := bufio.NewReader(bytes.NewReader(input))
	_, size, err := decodeSize(reader)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 10, size)
}

func TestDecodeSize01(t *testing.T) {
	input := []byte{0x42, 0xBC}
	reader := bufio.NewReader(bytes.NewReader(input))
	_, size, err := decodeSize(reader)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 700, size)
}

func TestDecodeSize10(t *testing.T) {
	input := []byte{0x80, 0x00, 0x00, 0x42, 0x68}
	reader := bufio.NewReader(bytes.NewReader(input))
	_, size, err := decodeSize(reader)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 17000, size)
}

func TestDecodeSize11(t *testing.T) {
	input := []byte{0xc0}
	reader := bufio.NewReader(bytes.NewReader(input))
	flag, size, err := decodeSize(reader)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 192, size)
	assert.EqualValues(t, 0b11, flag)
}

func TestDecodeStringNormal(t *testing.T) {
	input := []byte{0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21}
	reader := bufio.NewReader(bytes.NewReader(input))
	str, err := decodeString(reader)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "Hello, World!", str)
}

func TestDecodeString8BitInt(t *testing.T) {
	input := []byte{0xC0, 0x7B}
	reader := bufio.NewReader(bytes.NewReader(input))
	str, err := decodeString(reader)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "123", str)
}
