package main

import (
	"unicode"

	"github.com/codecrafters-io/redis-starter-go/resp"
)

func checkSimpleString(raw, respStr string) bool {
	parsed, err := resp.ParseSimpleString(raw)
	if err != nil {
		return false
	}

	return parsed == respStr
}

func toLowerString(data []byte) string {
	lowerBytes := make([]rune, len(data))
	for i, b := range data {
		lowerBytes[i] = unicode.ToLower(rune(b))
	}
	return string(lowerBytes)
}
