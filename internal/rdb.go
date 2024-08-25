package internal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"
)

const (
	rdbMetadataIndicator                 byte = 0xFA
	rdbDatabaseIndicator                 byte = 0xFE
	rdbHashtableSizeInformationIndicator byte = 0xFB
	rdbStringEncoding                    byte = 0x00
	rdbExpiryMilis                       byte = 0xFC
	rdbExpirySeconds                     byte = 0xFD
	rdbEndOfFile                         byte = 0xFF
)

type RDBReader struct {
	reader *bufio.Reader
}

type Metadata struct {
	Name  string
	Value string
}

func NewRDBReader() *RDBReader {
	return &RDBReader{}
}

func (r *RDBReader) LoadFile(filepath string) (storage, error) {
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		log.Println("No RDB file exists -> starting new DB")
		return nil, nil
	}

	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	r.reader = bufio.NewReader(file)

	header, err := r.readHeader()
	if err != nil {
		return nil, fmt.Errorf("error reading header: %w", err)
	}
	log.Println("Read header:", string(header))

	metas, err := r.readMetadata()
	if err != nil {
		return nil, fmt.Errorf("error reading metadata: %w", err)
	}
	log.Println("Read metadatas:", metas)

	// TODO: implement read multiple dbs
	dbIdx, data, err := r.readDatabase()
	if err != nil {
		return nil, fmt.Errorf("error reading database: %w", err)
	}
	log.Println("Keys loaded for db", dbIdx, ":", len(data))

	checksum, err := r.readEndOfFile()
	if err != nil {
		return nil, fmt.Errorf("error reading end of file: %w", err)
	}
	log.Println("RDB checksum:", checksum)

	return data, nil
}

func (r *RDBReader) readHeader() ([]byte, error) {
	buf, err := r.reader.ReadBytes(rdbMetadataIndicator)
	if err != nil {
		return buf, err
	}
	r.reader.UnreadByte()
	return buf[:len(buf)-1], nil
}

func (r *RDBReader) readMetadata() ([]Metadata, error) {
	metadatas := make([]Metadata, 0)
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			// if err == io.EOF {
			// 	break
			// }
			return nil, err
		}
		if b != rdbMetadataIndicator {
			r.reader.UnreadByte()
			break
		}

		name, err := decodeString(r.reader)
		if err != nil {
			return nil, fmt.Errorf("Error decoding string from metadata:: %w", err)
		}
		val, err := decodeString(r.reader)
		if err != nil {
			return nil, fmt.Errorf("Error decoding string from metadata:: %w", err)
		}
		metadatas = append(metadatas, Metadata{name, val})
	}
	return metadatas, nil
}

// Read the actual data -> db index, storage, expired storage, error if any
func (r *RDBReader) readDatabase() (int, storage, error) {
	var data storage
	b, err := r.reader.ReadByte()
	if err != nil {
		return -1, data, nil
	}
	if b != rdbDatabaseIndicator {
		r.reader.UnreadByte()
		return -1, data, fmt.Errorf("expect rdbDatabaseIndicator but got %v", b)
	}

	// Read the index of the database
	_, idx, err := decodeSize(r.reader)
	if err != nil {
		return idx, data, err
	}
	log.Println("DB index:", idx)

	b, err = r.reader.ReadByte()
	if err != nil {
		return idx, data, err
	}
	if b != rdbHashtableSizeInformationIndicator {
		return idx, data, fmt.Errorf("expect rdbHashtableSizeInformationIndicator but got %v", b)
	}

	_, dataHTSSize, err := decodeSize(r.reader)
	if err != nil {
		return idx, data, fmt.Errorf("error reading hash table size information:: %w", err)
	}
	_, _, err = decodeSize(r.reader)
	if err != nil {
		return idx, data, fmt.Errorf("error reading hash table size information:: %w", err)
	}
	data = make(storage, dataHTSSize)

Loop:
	// Read key, value pairs
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return idx, data, fmt.Errorf("Error reading key/value:: %w", err)
		}
		var val Value
		switch b {
		case rdbEndOfFile:
			r.reader.UnreadByte()
			break Loop // Meet end of file section
		case rdbExpiryMilis:
			expiry, err := decodeExpiryMilis(r.reader)
			if err != nil {
				return idx, data, fmt.Errorf("Error decoding expiry: %w", err)
			}
			val.ExpiredTimeMilli = int64(expiry)
		case rdbExpirySeconds:
			expiry, err := decodeExpirySeconds(r.reader)
			if err != nil {
				return idx, data, fmt.Errorf("Error decoding expiry: %w", err)
			}
			val.ExpiredTimeMilli = int64(expiry * 1000)
		default:
			r.reader.UnreadByte() // Unread the format byte
			key, valBytes, err := decodeKeyValue(r.reader)
			if err != nil {
				return idx, data, err
			}
			val.Value = string(valBytes)
			data[key] = val
		}
	}

	return idx, data, nil
}

// Return the checksum of the file
func (r *RDBReader) readEndOfFile() ([]byte, error) {
	b, err := r.reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("error reading end of file token: %w", err)
	}
	if b != rdbEndOfFile {
		return nil, fmt.Errorf("expect end of file token but get %v instead", b)
	}

	buf := make([]byte, 8)
	_, err = r.reader.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("error reading end of file: %w", err)
	}
	return buf, nil
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

func decodeExpirySeconds(reader *bufio.Reader) (uint32, error) {
	var expirySeconds uint32
	b, err := reader.ReadByte()
	if err != nil {
		return expirySeconds, err
	}

	if b != rdbExpirySeconds {
		return expirySeconds, fmt.Errorf("invalid expiry seconds byte")
	}

	buf := make([]byte, 4)
	_, err = reader.Read(buf)
	if err != nil {
		return expirySeconds, err
	}
	expirySeconds = binary.LittleEndian.Uint32(buf)
	return expirySeconds, nil
}

func decodeExpiryMilis(reader *bufio.Reader) (uint64, error) {
	var expirySeconds uint64
	b, err := reader.ReadByte()
	if err != nil {
		return expirySeconds, err
	}

	if b != rdbExpiryMilis {
		return expirySeconds, fmt.Errorf("innvalid expiry miliseconds byte")
	}

	buf := make([]byte, 8)
	_, err = reader.Read(buf)
	if err != nil {
		return expirySeconds, err
	}
	expirySeconds = binary.LittleEndian.Uint64(buf)
	return expirySeconds, nil
}

// TODO: Refactor to have this function decode all the types and the expiry as well
func decodeKeyValue(reader *bufio.Reader) (string, []byte, error) {
	var key string
	var val []byte
	b, err := reader.ReadByte()
	if err != nil {
		return "", val, err
	}
	switch b {
	case rdbStringEncoding, 0xC0, 0xC1, 0xC2:
		key, err = decodeString(reader)
		if err != nil {
			return "", val, err
		}
		valStr, err := decodeString(reader)
		if err != nil {
			return "", val, err
		}
		val = []byte(valStr)
	default:
		panic("Not implemented")
	}

	return key, val, nil
}

func Save(filepath string, db *DB) error {
	panic("Not implemented")
}
