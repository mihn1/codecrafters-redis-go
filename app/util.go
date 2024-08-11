package main

import (
	"github.com/codecrafters-io/redis-starter-go/resp"
)

func checkSimpleString(raw, respStr string) bool {
	parsed, err := resp.ParseSimpleString(raw)
	if err != nil {
		return false
	}

	return parsed == respStr
}
