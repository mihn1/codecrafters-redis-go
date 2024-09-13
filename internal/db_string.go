package internal

import "time"

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
