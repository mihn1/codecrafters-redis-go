package main

import "net"

type ConnectionID int64

type Connection struct {
	id   ConnectionID
	conn net.Conn
}

func NewConnection(id ConnectionID, conn net.Conn) *Connection {
	return &Connection{
		id:   id,
		conn: conn,
	}
}
