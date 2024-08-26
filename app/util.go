package main

import (
	"strings"
	"unicode"

	"github.com/codecrafters-io/redis-starter-go/resp"
)

func CheckSimpleString(raw, respStr string) bool {
	parsed, err := resp.ParseSimpleString(raw)
	if err != nil {
		return false
	}

	return parsed == respStr
}

func ToLowerString(data []byte) string {
	lowerBytes := make([]rune, len(data))
	for i, b := range data {
		lowerBytes[i] = unicode.ToLower(rune(b))
	}
	return string(lowerBytes)
}

func IsEmptyOrWhitespace(s string) bool {
	return len(s) == 0 || len(strings.TrimSpace(s)) == 0
}
