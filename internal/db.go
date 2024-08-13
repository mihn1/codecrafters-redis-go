package internal

import (
	"sync"
	"time"
)

type Value struct {
	Value       string
	CreatedAt   int64
	ExpiredTime int64
}

type DB struct {
	Options *DBOptions
	mapping map[string]Value
	mu      *sync.Mutex
}

func NewDB(options DBOptions) *DB {
	if options.ExpiryTime == 0 {
		options.ExpiryTime = 60_000 // 1 minute
	}

	return &DB{
		Options: &options,
		mapping: make(map[string]Value),
		mu:      &sync.Mutex{},
	}
}

func (db *DB) Get(key string) (Value, error) {
	if v, ok := db.mapping[key]; ok {
		if v.ExpiredTime < time.Now().UnixMilli() {
			go db.tryDelete(key)
			return Value{}, &KeyExpiredError{}
		}
		return v, nil
	}
	return Value{}, &KeyNotFoundError{}
}

func (db *DB) Set(key string, val string, expireAfterMillis int64) {
	value := Value{
		Value:       val,
		CreatedAt:   time.Now().UnixMilli(),
		ExpiredTime: time.Now().Add(time.Duration(expireAfterMillis) * time.Millisecond).UnixMilli(),
	}

	db.mu.Lock()
	db.mapping[key] = value
	db.mu.Unlock()
}

func (db *DB) tryDelete(key string) {
	db.mu.Lock()
	// Check again
	if v, ok := db.mapping[key]; ok {
		if v.ExpiredTime < time.Now().UnixMilli() {
			delete(db.mapping, key)
		}
	}
	db.mu.Unlock()
}
