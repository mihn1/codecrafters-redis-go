package internal

import (
	"sync"
	"time"
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

type Value struct {
	Value            []byte
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

func (db *DB) Get(key string) (Value, error) {
	if v, ok := db.storage[key]; ok {
		if v.ExpiredTimeMilli > 0 && v.ExpiredTimeMilli < time.Now().UnixMilli() {
			go db.tryDelete(key)
			return Value{}, &KeyExpiredError{}
		}
		return v, nil
	}
	return Value{}, &KeyNotFoundError{}
}

func (db *DB) Set(key string, val []byte, valType ValueType, expireAfterMilli int64) {
	value := Value{
		Value: val,
		Type:  valType,
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
