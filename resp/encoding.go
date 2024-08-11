package resp

import (
	"fmt"
	"strings"
)

const (
	NULL_BULK_STRING = "$-1\r\n"
	NULL_ARRAY       = "*-1\r\n"
)

func EncodeBulkString(val string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
}

func EncodeSimpleString(val string) string {
	return fmt.Sprintf("+%s\r\n", val)
}

func EncodeArrayBulkStrings(vals []string) string {
	bulkStrings := make([]string, 0, len(vals))
	for _, val := range vals {
		bulkStrings = append(bulkStrings, EncodeBulkString(val))
	}
	return fmt.Sprintf("*%d\r\n%s", len(vals), strings.Join(bulkStrings, ""))
}

func EncodeFile(buf []byte) string {
	return fmt.Sprintf("$%d\r\n%s", len(buf), string(buf))
}

func EncodeError(val string) string {
	return fmt.Sprintf("-%s\r\n", val)
}
