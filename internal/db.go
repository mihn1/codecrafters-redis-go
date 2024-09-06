package internal

import (
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
			},
			Type: ValTypeStream,
		}
	}

	var entryID StreamEntryID
	valueStream := v.Data.(*ValueStream)
	// TODO: validate the entryId
	parts := strings.Split(entryIDRaw, "-")
	switch len(parts) {
	case 1:
		if parts[0] != "*" {
			return "", &StreamKeyInvalid{}
		}
		// TODO: generate key
	case 2:
		ts, err := strconv.Atoi(parts[0])
		if err != nil {
			return "", &StreamKeyInvalid{}
		}
		seq, err := strconv.Atoi(parts[1])
		if err != nil {
			return "", &StreamKeyInvalid{}
		}
		entryID = StreamEntryID{
			Timestamp: int64(ts),
			Sequence:  int64(seq),
		}

		// Validate the key
		if len(valueStream.keys) > 0 {
			lastKey := valueStream.keys[len(valueStream.keys)-1]
			if lastKey.Timestamp > entryID.Timestamp || (lastKey.Timestamp == entryID.Timestamp && lastKey.Sequence >= entryID.Sequence) {
				return "", &StreamKeyTooSmall{}
			}
		}
	default:
		return "", &StreamKeyInvalid{}
	}

	valueStream.keys = append(valueStream.keys, entryID)
	valueStream.values[entryID] = data

	db.mu.Lock()
	defer db.mu.Unlock()
	db.storage[key] = v
	return entryIDRaw, nil
}
