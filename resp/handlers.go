package resp

import (
	"fmt"

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
	return buildSimpleString("PONG")
}

func echo(db *internal.DB, args []string) string {
	return buildBulkString(args[0])
}

func get(db *internal.DB, args []string) string {
	if len(args) < 1 {
		return fmt.Sprintf("-ERR wrong number of arguments for '%s' command\r\n", args[0])
	}
	if v, ok := db.Mapping[args[0]]; ok {
		return buildBulkString(v)
	}
	return buildBulkString("")
}

func set(db *internal.DB, args []string) string {
	if len(args) < 2 {
		return fmt.Sprintf("-ERR wrong number of arguments for '%s' command\r\n", args[0])
	}
	db.Mapping[args[0]] = args[1]
	return buildSimpleString("OK")
}

func unknown(db *internal.DB, args []string) string {
	return buildError("ERR unknown command '" + args[0] + "'")
}

func buildBulkString(val string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
}

func buildSimpleString(val string) string {
	return fmt.Sprintf("+%s\r\n", val)
}

func buildError(val string) string {
	return fmt.Sprintf("-%s\r\n", val)
}
