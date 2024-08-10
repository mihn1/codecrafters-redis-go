package resp

import (
	"fmt"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal"
)

var commandHandlers = map[CommandType]func(*internal.DB, []string) string{
	Ping:    ping,
	Echo:    echo,
	Set:     set,
	Get:     get,
	Unknown: unknown,
}

func HandleCommand(db *internal.DB, command Command) string {
	return commandHandlers[command.CommandType](db, command.Agrs)
}

func ping(db *internal.DB, args []string) string {
	return encodeSimpleString("PONG")
}

func echo(db *internal.DB, args []string) string {
	return encodeBulkString(args[0])
}

func get(db *internal.DB, args []string) string {
	if len(args) < 1 {
		return encodeError(fmt.Sprintf("ERR wrong number of arguments for GET: %v", len(args)))
	}

	v, err := db.Get(args[0])
	if err != nil {
		switch e := err.(type) {
		case *internal.KeyNotFoundError:
			return encodeBulkString("")
		case *internal.KeyExpiredError:
			return NULL_BULK_STRING
		default:
			return encodeError(e.Error())
		}
	}

	return encodeBulkString(v.Value)
}

func set(db *internal.DB, args []string) string {
	if len(args) != 2 && len(args) != 4 {
		return encodeError(fmt.Sprintf("ERR wrong number of arguments for SET: %v", len(args)))
	}

	key := args[0]
	var expiry time.Time

	// resolve expiry
	if len(args) == 4 {
		expiryType := args[2]
		expiryNum, err := strconv.Atoi(args[3])
		if err != nil {
			return encodeError(err.Error())
		}

		expiry, err = resolveExpiry(expiryType, expiryNum)
		if err != nil {
			return encodeError(err.Error())
		}
	} else {
		expiry = time.Now().Add(time.Duration(db.Options.ExpireTime) * time.Millisecond)
	}

	value := internal.Value{Value: args[1], ExpiredTime: expiry}
	db.Set(key, value)
	return encodeSimpleString("OK")
}

func unknown(db *internal.DB, args []string) string {
	return encodeError("ERR unknown command")
}

func resolveExpiry(expiryType string, expiryNum int) (time.Time, error) {
	switch expiryType {
	case "px":
		return time.Now().Add(time.Duration(expiryNum) * time.Millisecond), nil
	case "ex":
		return time.Now().Add(time.Duration(expiryNum) * time.Second), nil
	default:
		return time.Now(), fmt.Errorf("invalid expiry type")
	}
}
