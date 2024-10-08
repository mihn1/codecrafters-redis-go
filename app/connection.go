package main

import (
	"bufio"
	"net"
)

type Batch struct {
	isError      bool
	handlerQueue []commandHandler
	commandQueue []*Command
}

type ConnectionID int64

type Connection struct {
	id      ConnectionID
	conn    net.Conn
	reader  *bufio.Reader
	isBatch bool
	batch   *Batch
}

func NewConnection(id ConnectionID, conn net.Conn) *Connection {
	return &Connection{
		id:      id,
		conn:    conn,
		reader:  bufio.NewReader(conn),
		isBatch: false,
	}
}

func (c *Connection) Close() error {
	return c.conn.Close()
}

func (c *Connection) sendBytes(bytes []byte) error {
	_, err := c.conn.Write(bytes)
	return err
}

func (c *Connection) readResponse(buf []byte) ([]byte, error) {
	n, err := c.reader.Read(buf)
	if err != nil {
		return []byte{}, err
	}

	return buf[:n], nil
}
