package internal

import "time"

type Value struct {
	Value       string
	ExpiredTime time.Time
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
		if v.ExpiredTime.Before(time.Now()) {
			return Value{}, &KeyExpiredError{}
		}
		return v, nil
	}
	return Value{}, &KeyNotFoundError{}
}

func (db *DB) Set(key string, value Value) {
	db.Mapping[key] = value
}
