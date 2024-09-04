package internal

import (
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
			Data: make(ValueStream),
			Type: ValTypeStream,
		}
	}

	// TODO: validate the entryId
	var entryID StreamEntryID
	streamData := v.Data.(ValueStream)
	streamData[entryID] = data

	db.mu.Lock()
	defer db.mu.Unlock()
	db.storage[key] = v
	return entryIDRaw, nil
}
