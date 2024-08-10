package resp

import (
	"fmt"
	"strings"
)

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

func encodeArrayBulkStrings(vals []string) string {
	bulkStrings := make([]string, 0, len(vals))
	for _, val := range vals {
		bulkStrings = append(bulkStrings, encodeBulkString(val))
	}
	return fmt.Sprintf("*%d\r\n%s", len(vals), strings.Join(bulkStrings, ""))
}

func encodeError(val string) string {
	return fmt.Sprintf("-%s\r\n", val)
}
