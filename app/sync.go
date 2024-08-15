package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/resp"
)

func syncWithMaster(s *Server) (*Connection, error) {
	connection, err := handshake(s)
	// if err == nil {
	// 	log.Println("Handling master connection")
	// 	s.asSlave.masterConnection = connection
	// 	go s.handleConnection(s.asSlave.masterConnection)
	// }
	return connection, err
}

func handshake(s *Server) (*Connection, error) {
	masterAddr := fmt.Sprintf("%s:%d", s.asSlave.masterHost, s.asSlave.masterPort)
	log.Println("Syncing with master...", masterAddr)
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		log.Fatalf("Error connecting to master: %v", err)
		return nil, err
	}
	connection := NewConnection(getConnID(), conn)

	log.Println("Sending PING to master")
	err = sendPing(connection)
	if err != nil {
		log.Fatalf("Error sending PING: %v", err)
		return connection, err
	}

	log.Println("Sending replication config to master")
	err = sendReplConfig(s, connection)
	if err != nil {
		log.Fatalf("Error sending replication config: %v", err)
		return connection, err
	}

	log.Println("Sending PSYNC to master")
	err = sendPSYNC(s, connection)
	if err != nil {
		log.Fatalf("Error sending PSYNC: %v", err)
		return connection, err
	}

	return connection, nil
}

func sendPing(c *Connection) error {
	err := c.sendBytes(resp.EncodeArrayBulkStrings([]string{"PING"}))
	if err != nil {
		return err
	}

	buf := make([]byte, 32)
	res, err := c.readResponse(buf)
	if err != nil {
		return err
	}

	if !checkSimpleString(string(res), PONG) {
		return fmt.Errorf("unable to PING: %s", res)
	}

	return nil
}

func sendReplConfig(s *Server, c *Connection) error {
	message := resp.EncodeArrayBulkStrings([]string{"REPLCONF", "listening-port", strconv.Itoa(s.port)})
	err := c.sendBytes(message)
	if err != nil {
		return err
	}

	buf := make([]byte, 32)
	res, err := c.readResponse(buf)
	if err != nil {
		return err
	}

	if !checkSimpleString(string(res), OK) {
		return fmt.Errorf("unable to REPLCONF listening-port: %s", res)
	}

	message = resp.EncodeArrayBulkStrings([]string{"REPLCONF", "capa", "psync2"})
	err = c.sendBytes(message)
	if err != nil {
		return err
	}

	res, err = c.readResponse(buf)
	if err != nil {
		return err
	}

	if !checkSimpleString(string(res), OK) {
		return fmt.Errorf("unable to REPLCONF listening-port: %s", res)
	}

	return nil
}

func sendPSYNC(_ *Server, c *Connection) error {
	err := c.sendBytes(resp.EncodeArrayBulkStrings([]string{"PSYNC", "?", "-1"}))
	if err != nil {
		return err
	}

	res, err := resp.ReadNextResp(c.reader)
	if err != nil && err != io.EOF {
		log.Fatalln("Error reading PSYNC response", err)
		return err
	}

	log.Println("PSYNC response:", string(res.Data[0]))

	b, err := resp.ReadLine(c.reader)
	if err != nil {
		return err
	}
	log.Printf("PSYNC file metadata response: %q\n", string(b))
	size, err := strconv.Atoi(string(b[1 : len(b)-2]))
	if err != nil {
		return err
	}
	buf := make([]byte, size)
	// TODO: use resp parser to read this file

	n, err := c.reader.Read(buf)
	log.Printf("PSYNC file response %d bytes\n", n)
	if err != nil {
		return err
	}

	return nil
}

func sendGETACK(slave *Slave) error {
	message := resp.EncodeArrayBulkStrings([]string{string(ReplConf), "GETACK", "*"})
	return slave.connection.sendBytes(message)

	// rp, err := resp.ReadNextResp(slave.connection.reader)
	// if err != nil {
	// 	return ackOffset, err
	// }

	// log.Println("GETACK response:", string(rp.Raw))
	// if rp.Type != resp.INTEGER {
	// 	return ackOffset, fmt.Errorf("expect INTEGER, getting %v", rp.Type)
	// }

	// if len(rp.Data) != 1 {
	// 	return ackOffset, fmt.Errorf("invalid ACk data")
	// }

	// ackOffset, err = strconv.ParseInt(string(rp.Data[0]), 10, 64)
	// if err != nil {
	// 	return ackOffset, err
	// }

	// log.Printf("Updating slave %v offset to %v\n", slave.connection.id, ackOffset)
	// slave.ackOffset = ackOffset
	// return ackOffset, nil
}
