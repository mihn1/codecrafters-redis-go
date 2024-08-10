package internal

import (
	"time"
)

type Value struct {
	Value       string
	CreatedAt   int64
	ExpiredTime int64
}

type DB struct {
	Options *DBOptions
	Mapping map[string]Value
}

func NewDB(options DBOptions) *DB {
	if options.ExpiryTime == 0 {
		options.ExpiryTime = 60_000 // 1 minute
	}

	return &DB{
		Options: &options,
		Mapping: make(map[string]Value),
	}
}

func (db *DB) Get(key string) (Value, error) {
	if v, ok := db.Mapping[key]; ok {
		if v.ExpiredTime < time.Now().UnixMilli() {
			delete(db.Mapping, key)
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
	db.Mapping[key] = value
}
