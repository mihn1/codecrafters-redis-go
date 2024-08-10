package resp

import "fmt"

const (
	NULL_BULK_STRING = "$-1\r\n"
	NULL_ARRAY       = "*-1\r\n"
)

func encodeBulkString(val string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
}

func encodeSimpleString(val string) string {
	return fmt.Sprintf("+%s\r\n", val)
}

func encodeError(val string) string {
	return fmt.Sprintf("-%s\r\n", val)
}
