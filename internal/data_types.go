package internal

import (
	"strconv"
	"sync"
)

type ValueType byte

const (
	ValTypeString ValueType = 0x0
	ValTypeList   ValueType = 0x1
	ValTypeSet    ValueType = 0x2
	ValTypeZSet   ValueType = 0x3
	ValTypeHash   ValueType = 0x4
	ValTypeStream ValueType = 0x5
)

type ValueData interface {
	ToBytes() []byte
}

// String type
type ValueString []byte

func (v ValueString) ToBytes() []byte {
	return v
}

// Stream type
type StreamEntryData map[string][]byte

type StreamEntryID struct {
	Timestamp uint64
	Sequence  uint64
}

func (e StreamEntryID) String() string {
	return strconv.FormatUint(e.Timestamp, 10) + "-" + strconv.FormatUint(e.Sequence, 10)
}

type ValueStream struct {
	keys   []StreamEntryID
	values map[StreamEntryID]StreamEntryData
	mu     sync.RWMutex
}

func (v ValueStream) ToBytes() []byte {
	bytes := make([]byte, 0)
	return bytes
}
