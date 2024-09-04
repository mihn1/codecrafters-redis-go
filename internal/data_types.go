package internal

import "strconv"

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

type ValueString []byte

func (v ValueString) ToBytes() []byte {
	return v
}

type StreamEntryData map[string][]byte

type StreamEntryID struct {
	Timestamp  int64
	SequenceNo int64
}

func (e StreamEntryID) String() string {
	return strconv.FormatInt(e.Timestamp, 10) + "-" + strconv.FormatInt(e.SequenceNo, 10)
}

type ValueStream map[StreamEntryID]StreamEntryData

func (v ValueStream) ToBytes() []byte {
	bytes := make([]byte, 0)
	return bytes
}
