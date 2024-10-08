package resp

import (
	"strconv"
)

const (
	null_bulk_string = "$-1\r\n"
	null_array       = "*-1\r\n"
	error_prefix     = "ERR "
)

func EncodeNullBulkString() []byte {
	return []byte(null_bulk_string)
}

func EncodeNullArray() []byte {
	return []byte(null_bulk_string)
}

func EncodeBulkString(val string) []byte {
	res := make([]byte, 0, len(val)+6)
	res = append(res, byte(BULK_STRING))
	res = strconv.AppendInt(res, int64(len(val)), 10)
	res = append(res, '\r', '\n')
	res = append(res, val...)
	return append(res, '\r', '\n')
}

func EncodeSimpleString(val string) []byte {
	res := make([]byte, 0, len(val)+3)
	res = append(res, byte(SIMPLE_STRING))
	res = append(res, val...)
	return append(res, '\r', '\n')
}

func EncodeInterger(val int64) []byte {
	res := make([]byte, 0, 16)
	res = append(res, byte(INTEGER))
	res = strconv.AppendInt(res, val, 10)
	return append(res, '\r', '\n')
}

func EncodeArray(vals [][]byte) []byte {
	res := make([]byte, 0, len(vals)+3)
	res = append(res, byte(ARRAY))
	res = strconv.AppendInt(res, int64(len(vals)), 10)
	res = append(res, '\r', '\n')
	for _, val := range vals {
		res = append(res, val...)
	}
	return res
}

func EncodeArrayBulkStrings(vals []string) []byte {
	res := make([]byte, 0, len(vals)+3)
	res = append(res, byte(ARRAY))
	res = strconv.AppendInt(res, int64(len(vals)), 10)
	res = append(res, '\r', '\n')
	for _, val := range vals {
		res = append(res, EncodeBulkString(val)...)
	}
	return res
}

func EncodeFile(buf []byte) []byte {
	res := make([]byte, 0, len(buf)+4)
	res = append(res, '$')
	res = strconv.AppendInt(res, int64(len(buf)), 10)
	res = append(res, '\r', '\n')
	res = append(res, buf...)
	return res
}

func EncodeError(val string) []byte {
	res := make([]byte, 0, len(val)+7)
	res = append(res, byte(ERROR))
	res = append(res, error_prefix...)
	res = append(res, val...)
	return append(res, '\r', '\n')
}

func EncodeErrorNoPrefix(val string) []byte {
	res := make([]byte, 0, len(val)+3)
	res = append(res, byte(ERROR))
	res = append(res, val...)
	return append(res, '\r', '\n')
}
