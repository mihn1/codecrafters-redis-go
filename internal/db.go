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
