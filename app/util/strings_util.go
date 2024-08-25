package util

import "strings"

func IsEmptyOrWhitespace(s string) bool {
	return len(s) == 0 || len(strings.TrimSpace(s)) == 0
}
