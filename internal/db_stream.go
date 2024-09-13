package internal

import (
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
	stream := v.Data.(*ValueStream)
	stream.mu.Lock()
	defer stream.mu.Unlock()

	parts := strings.Split(entryIDRaw, "-")
	var lastKey *StreamEntryID
	if len(stream.keys) > 0 {
		lastKey = &stream.keys[len(stream.keys)-1]
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
	stream.keys = append(stream.keys, entryID)
	stream.values[entryID] = data

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
	stream.mu.RLock()
	defer stream.mu.RUnlock()

	startIndex, err := streamFindStartIndex(stream, start, false)
	if err != nil {
		return nil, nil, err
	}
	endIndex, err := streamFindEndIndex(stream, end)
	if err != nil {
		return nil, nil, err
	}

	for i := startIndex; i <= endIndex; i++ {
		ids = append(ids, stream.keys[i])
		values = append(values, stream.values[stream.keys[i]])
	}

	return ids, values, nil
}

func (db *DB) StreamRead(keys, starts []string, blockMilli int64) []XReadKeyResult {
	time.Sleep(time.Duration(blockMilli) * time.Millisecond)

	res := make([]XReadKeyResult, 0, len(keys))
	for i := 0; i < len(keys); i++ {
		entryIDs, entryVals, err := db.streamReadSingle(keys[i], starts[i])
		if err != nil {
			continue
		}
		res = append(res, XReadKeyResult{Key: keys[i], EntryIDs: entryIDs, EntryValues: entryVals})
	}
	return res
}

func (db *DB) streamReadSingle(key, start string) ([]StreamEntryID, []StreamEntryData, error) {
	v, err := db.checkKey(key, ValTypeStream)
	if err != nil || v.Type != ValTypeStream {
		return nil, nil, &KeyNotFoundError{}
	}

	stream := v.Data.(*ValueStream)
	stream.mu.RLock()
	defer stream.mu.RUnlock()

	startIndex, err := streamFindStartIndex(stream, start, true)
	if err != nil {
		return nil, nil, err
	}

	endIndex := len(stream.keys) - 1

	// XRead reads entries having strictly greater ID than the one specified in from
	entryIDs := stream.keys[startIndex : endIndex+1]
	entryVals := make([]StreamEntryData, len(entryIDs))
	for i, id := range entryIDs {
		entryVals[i] = stream.values[id]
	}
	return entryIDs, entryVals, nil
}

func streamParseEntryID(idRaw string, isEnd bool) (StreamEntryID, error) {
	parts := strings.Split(idRaw, "-")
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

func streamFindStartIndex(stream *ValueStream, idRaw string, strict bool) (int, error) {
	if idRaw == "-" {
		return 0, nil
	}
	id, err := streamParseEntryID(idRaw, false)
	if err != nil {
		return 0, err
	}
	// lower bound binary search
	l, r := 0, len(stream.keys)
	for l < r {
		m := (l + r) / 2
		if compareEntryIds(stream.keys[m], id) < 0 {
			l = m + 1
		} else {
			r = m
		}
	}
	if strict && compareEntryIds(stream.keys[l], id) == 0 {
		return l + 1, nil
	}
	return l, nil
}

func streamFindEndIndex(stream *ValueStream, idRaw string) (int, error) {
	if idRaw == "+" {
		return len(stream.keys) - 1, nil
	}
	id, err := streamParseEntryID(idRaw, true)
	if err != nil {
		return 0, err
	}
	// upper bound binary search
	l, r := 0, len(stream.keys)
	for l < r {
		m := (l + r) / 2
		if compareEntryIds(stream.keys[m], id) > 0 {
			r = m
		} else {
			l = m + 1
		}
	}
	return l - 1, nil
}

func compareEntryIds(first, second StreamEntryID) int {
	if first.Timestamp < second.Timestamp {
		return -1
	} else if first.Timestamp > second.Timestamp {
		return 1
	} else if first.Sequence < second.Sequence {
		return -1
	} else if first.Sequence > second.Sequence {
		return 1
	} else {
		return 0
	}
}
