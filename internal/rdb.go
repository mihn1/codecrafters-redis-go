package internal

import (
	"bufio"
	"encoding/binary"
	"os"
	"strconv"
)

const (
	rdbMetadataIndicator byte = 0xFA
	rdbDatabaseIndicator byte = 0xFE
	rdbStringEncoding    byte = 0x00
	rdbExpiryMilis       byte = 0xFC
	rdbExpirySeconds     byte = 0xFD
	rdbEndOfFile         byte = 0xFF
)

type RDB struct {
	reader *bufio.Reader
}

func NewRDB(dir, dbfilename string) *RDB {
	return &RDB{}
}

func (r *RDB) LoadFile(filepath string) (map[string]Value, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data map[string]Value

	return data, nil
}

func (r *RDB) readHeader() ([]byte, error) {
	buf := make([]byte, 9)
	_, err := r.reader.Read(buf)
	return buf, err
}

func (r *RDB) readMetadata() {

}

func (r *RDB) readDatabase() {

}

func (r *RDB) readEndOfFile() {

}

// Return the first 2 bits of the next byte, the parsed size, and the error if any
func decodeSize(reader *bufio.Reader) (byte, int, error) {
	curByte, err := reader.ReadByte()
	if err != nil {
		return 0, -1, err
	}

	var size int
	flag := curByte >> 6
	switch flag {
	case 0b00:
		size = int(curByte) // just return as is
	case 0b11:
		size = int(curByte)
	case 0b01:
		nxtByte, err := reader.ReadByte()
		if err != nil {
			return flag, -1, err
		}
		size = int((curByte & 0b00111111)) // take 6 first bits
		size <<= 8
		size |= int(nxtByte)
	case 0b10:
		var size32 int32
		err = binary.Read(reader, binary.BigEndian, &size32)
		if err != nil {
			return flag, -1, err
		}
		size = int(size32)
	}

	return flag, size, nil
}

func decodeString(reader *bufio.Reader) (string, error) {
	flag, size, err := decodeSize(reader)
	if err != nil {
		return "", err
	}

	var str string
	// First 2 bits of size are 0b11
	switch flag {
	case 0b11:
		// size now detemines the format of the string
		switch size {
		case 0xC0:
			// String is an 8-bit integer
			b, err := reader.ReadByte()
			if err != nil {
				return "", err
			}
			str = strconv.Itoa(int(b))
		case 0xC1:
			// string is a 16-bit integer
			buf := make([]byte, 2)
			_, err = reader.Read(buf)
			if err != nil {
				return "", err
			}
			str = strconv.FormatUint(uint64(binary.LittleEndian.Uint16(buf)), 10)
		case 0xC2:
			// string is a 32-bit integer
			buf := make([]byte, 4)
			_, err = reader.Read(buf)
			if err != nil {
				return "", err
			}
			str = strconv.FormatUint(uint64(binary.LittleEndian.Uint32(buf)), 10)
		default:
			// LZF compression algorithm
			panic("LZF: Not implemented")
		}

	default:
		buf := make([]byte, size)
		_, err := reader.Read(buf)
		if err != nil {
			return "", err
		}

		str = string(buf[:size])
	}
	return str, nil
}

func Save(filepath string, db *DB) error {
	panic("Not implemented")
}
