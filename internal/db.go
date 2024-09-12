package internal

import (
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Value struct {
	Data             ValueData
	Type             ValueType
	ExpiredTimeMilli int64
}

type storage map[string]Value

type DB struct {
	Options *DBOptions
	storage storage
	mu      *sync.RWMutex
}

func NewDB(options DBOptions) *DB {
	if options.ExpiryTime == 0 {
		options.ExpiryTime = 60_000 // 1 minute
	}

	return &DB{
		Options: &options,
		storage: make(map[string]Value),
		mu:      &sync.RWMutex{},
	}
}

func (db *DB) Snapshot() storage {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.storage
}

func (db *DB) InitStorage(data storage) {
	db.storage = data
}

func (db *DB) tryDelete(key string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	// Check again
	if v, ok := db.storage[key]; ok {
		if v.ExpiredTimeMilli < time.Now().UnixMilli() {
			delete(db.storage, key)
		}
	}
}

func (db *DB) checkKey(key string, valType ValueType) (Value, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if v, ok := db.storage[key]; ok {
		if v.ExpiredTimeMilli > 0 && v.ExpiredTimeMilli < time.Now().UnixMilli() {
			go db.tryDelete(key)
			return Value{}, &KeyExpiredError{}
		}
		if v.Type != valType {
			return Value{}, &TypeMismatchError{}
		}
		return v, nil
	}
	return Value{}, &KeyNotFoundError{}
}

func (db *DB) GetVal(key string) (Value, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if v, ok := db.storage[key]; ok {
		if v.ExpiredTimeMilli > 0 && v.ExpiredTimeMilli < time.Now().UnixMilli() {
			go db.tryDelete(key)
			return Value{}, &KeyExpiredError{}
		}
		return v, nil
	}
	return Value{}, &KeyNotFoundError{}
}

/*
Functions for strings type
*/
func (db *DB) StringGet(key string) (Value, error) {
	v, err := db.checkKey(key, ValTypeString)
	return v, err

}

func (db *DB) StringSet(key string, val []byte, expireAfterMilli int64) {
	value := Value{
		Data: ValueString(val),
		Type: ValTypeString,
	}
	if expireAfterMilli > 0 {
		value.ExpiredTimeMilli = time.Now().Add(time.Duration(expireAfterMilli) * time.Millisecond).UnixMilli()
	} else {
		value.ExpiredTimeMilli = 0 // No expire time
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	db.storage[key] = value
}

/*
Functions for stream type
*/
func (db *DB) StreamAdd(key string, entryIDRaw string, data StreamEntryData, expireAfterMilli int64) (string, error) {
	// Check if key exists
	v, err := db.checkKey(key, ValTypeStream)
	if err != nil || v.Type != ValTypeStream {
		v = Value{
			Data: &ValueStream{
				keys:   make([]StreamEntryID, 0),
				values: make(map[StreamEntryID]StreamEntryData),
				mu:     &sync.RWMutex{},
			},
			Type: ValTypeStream,
		}
	}

	var entryID StreamEntryID
	valueStream := v.Data.(*ValueStream)
	valueStream.mu.Lock()
	defer valueStream.mu.Unlock()

	// TODO: validate the entryId
	parts := strings.Split(entryIDRaw, "-")
	var lastKey *StreamEntryID
	if len(valueStream.keys) > 0 {
		lastKey = &valueStream.keys[len(valueStream.keys)-1]
	}

	switch len(parts) {
	case 1:
		if parts[0] != "*" {
			return "", &StreamKeyInvalid{}
		}
		curTS := uint64(time.Now().UnixMilli())
		if lastKey != nil {
			if curTS > lastKey.Timestamp {
				entryID.Timestamp = curTS
			} else {
				entryID.Timestamp = lastKey.Timestamp + 1
				entryID.Sequence = lastKey.Sequence + 1
			}
		} else {
			entryID.Timestamp = curTS
		}
	case 2:
		ts, err := strconv.ParseUint(parts[0], 10, 0)
		if err != nil {
			return "", &StreamKeyInvalid{}
		}

		var seq uint64
		if parts[1] == "*" { // Generate sequence
			if lastKey != nil {
				if ts < lastKey.Timestamp {
					return "", &StreamKeyTooSmall{}
				} else if ts == lastKey.Timestamp {
					seq = lastKey.Sequence + 1
				}
			} else if ts == 0 {
				// Default sequence is 0 except for timestamp == 0 then default sequence is 1
				seq = 1
			}
		} else { // Fully provided stream id
			seq, err = strconv.ParseUint(parts[1], 10, 0)
			if err != nil {
				return "", &StreamKeyInvalid{}
			}
		}

		entryID.Timestamp = ts
		entryID.Sequence = seq
	default:
		return "", &StreamKeyInvalid{}
	}

	// Validate the key
	if entryID.Timestamp == 0 && entryID.Sequence == 0 {
		return "", &StreamKeyInvalid{message: "The ID specified in XADD must be greater than 0-0"}
	}
	if lastKey != nil {
		if lastKey.Timestamp > entryID.Timestamp || (lastKey.Timestamp == entryID.Timestamp && lastKey.Sequence >= entryID.Sequence) {
			return "", &StreamKeyTooSmall{}
		}
	}
	valueStream.keys = append(valueStream.keys, entryID)
	valueStream.values[entryID] = data

	db.mu.Lock()
	defer db.mu.Unlock()
	db.storage[key] = v
	return entryID.String(), nil
}

func (db *DB) StreamRange(key, start, end string) ([]StreamEntryID, []StreamEntryData, error) {
	var ids []StreamEntryID
	var values []StreamEntryData

	v, err := db.checkKey(key, ValTypeStream)
	if err != nil || v.Type != ValTypeStream {
		return nil, nil, &KeyNotFoundError{}
	}

	stream := v.Data.(*ValueStream)
	startID, err := streamParseEntryID(start, false)
	if err != nil {
		return nil, nil, err
	}
	endID, err := streamParseEntryID(end, true)
	if err != nil {
		return nil, nil, err
	}

	startIndex := streamFindStartIndex(stream, startID)
	endIndex := streamFindEndIndex(stream, endID)
	for i := startIndex; i <= endIndex; i++ {
		ids = append(ids, stream.keys[i])
		values = append(values, stream.values[stream.keys[i]])
	}
	return ids, values, nil
}

func streamParseEntryID(entryID string, isEnd bool) (StreamEntryID, error) {
	parts := strings.Split(entryID, "-")
	switch len(parts) {
	case 1:
		ts, err := strconv.ParseUint(parts[0], 10, 0)
		if err != nil {
			return StreamEntryID{}, &StreamKeyInvalid{}
		}
		id := StreamEntryID{
			Timestamp: ts,
			Sequence:  0,
		}
		if isEnd {
			id.Sequence = math.MaxUint64
		}
		return id, nil
	case 2:
		ts, err := strconv.ParseUint(parts[0], 10, 0)
		if err != nil {
			return StreamEntryID{}, &StreamKeyInvalid{}
		}
		seq, err := strconv.ParseUint(parts[1], 10, 0)
		if err != nil {
			return StreamEntryID{}, &StreamKeyInvalid{}
		}
		return StreamEntryID{
			Timestamp: ts,
			Sequence:  seq,
		}, nil
	default:
		return StreamEntryID{}, &StreamKeyInvalid{}
	}
}

func streamFindStartIndex(stream *ValueStream, id StreamEntryID) int {
	// lower bound binary search
	l, r := 0, len(stream.keys)
	for l < r {
		m := (l + r) / 2
		if stream.keys[m].Timestamp < id.Timestamp ||
			(stream.keys[m].Timestamp == id.Timestamp && stream.keys[m].Sequence < id.Sequence) {
			l = m + 1
		} else {
			r = m
		}
	}
	return l
}

func streamFindEndIndex(stream *ValueStream, id StreamEntryID) int {
	// upper bound binary search
	l, r := 0, len(stream.keys)
	for l < r {
		m := (l + r) / 2
		if stream.keys[m].Timestamp > id.Timestamp || (stream.keys[m].Timestamp == id.Timestamp && stream.keys[m].Sequence > id.Sequence) {
			r = m
		} else {
			l = m + 1
		}
	}
	return l - 1
}
