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

func TestDecodeStringAsInt_8bit(t *testing.T) {
	input := []byte{0xC0, 0x7B}
	reader := bufio.NewReader(bytes.NewReader(input))
	str, err := decodeString(reader)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "123", str)
}

func TestDecodeStringAsInt_16bit(t *testing.T) {
	input := []byte{0xC1, 0x39, 0x30}
	reader := bufio.NewReader(bytes.NewReader(input))
	str, err := decodeString(reader)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "12345", str)
}

func TestDecodeStringAsInt_32bit(t *testing.T) {
	input := []byte{0xC2, 0x87, 0xD6, 0x12, 0x00}
	reader := bufio.NewReader(bytes.NewReader(input))
	str, err := decodeString(reader)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "1234567", str)
}

func TestReadMetadata(t *testing.T) {
	input := []byte{
		0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D,
		0x76, 0x65, 0x72, 0x06, 0x36, 0x2E, 0x30, 0x2E, 0x31,
		0x36,
		0xFE,
	}
	reader := bufio.NewReader(bytes.NewReader(input))
	rdb := RDBReader{reader: reader}

	metas, err := rdb.readMetadata()
	if err != nil {
		t.Fatal("Error:", err)
	}
	t.Log("Metas", metas)
	assert.Equal(t, len(metas), 1)
	assert.Equal(t, metas[0].Name, "redis-ver")
	assert.Equal(t, metas[0].Value, "6.0.16")

}

func TestLoadFile(t *testing.T) {
	filepath := "../dump.rdb"
	rdbReader := &RDBReader{}
	data, err := rdbReader.LoadFile(filepath)
	if err != nil {
		t.Fatal("Error:", err)
	}
	t.Log("Data:", data)
	assert.Equal(t, 3, len(data))
}
