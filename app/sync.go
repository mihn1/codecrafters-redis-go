package main

import (
	"bufio"
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
	connection := &Connection{
		id:   getConnID(),
		conn: conn,
	}

	log.Println("Sending PING to master")
	err = sendPing(conn)
	if err != nil {
		log.Fatalf("Error sending PING: %v", err)
		return connection, err
	}

	log.Println("Sending replication config to master")
	err = sendReplConfig(s, conn)
	if err != nil {
		log.Fatalf("Error sending replication config: %v", err)
		return connection, err
	}

	log.Println("Sending PSYNC to master")
	err = sendPSYNC(s, conn)
	if err != nil {
		log.Fatalf("Error sending PSYNC: %v", err)
		return connection, err
	}

	return connection, nil
}

func sendPing(conn net.Conn) error {
	err := sendBytes(conn, resp.EncodeArrayBulkStrings([]string{"PING"}))
	if err != nil {
		return err
	}

	buf := make([]byte, 32)
	res, err := readResponse(conn, buf)
	if err != nil {
		return err
	}

	if !checkSimpleString(res, PONG) {
		return fmt.Errorf("unable to PING: %s", res)
	}

	return nil
}

func sendReplConfig(s *Server, conn net.Conn) error {
	message := resp.EncodeArrayBulkStrings([]string{"REPLCONF", "listening-port", strconv.Itoa(s.port)})
	err := sendBytes(conn, message)
	if err != nil {
		return err
	}

	buf := make([]byte, 32)
	res, err := readResponse(conn, buf)
	if err != nil {
		return err
	}

	if !checkSimpleString(res, OK) {
		return fmt.Errorf("unable to REPLCONF listening-port: %s", res)
	}

	message = resp.EncodeArrayBulkStrings([]string{"REPLCONF", "capa", "psync2"})
	err = sendBytes(conn, message)
	if err != nil {
		return err
	}

	res, err = readResponse(conn, buf)
	if err != nil {
		return err
	}

	if !checkSimpleString(res, OK) {
		return fmt.Errorf("unable to REPLCONF listening-port: %s", res)
	}

	return nil
}

func sendPSYNC(_ *Server, conn net.Conn) error {
	err := sendBytes(conn, resp.EncodeArrayBulkStrings([]string{"PSYNC", "?", "-1"}))
	if err != nil {
		return err
	}

	reader := bufio.NewReader(conn)

	res, err := resp.ReadNextResp(reader)
	if err != nil && err != io.EOF {
		log.Fatalln("Error reading PSYNC response", err)
		return err
	}

	log.Println("PSYNC response:", string(res.Data[0]))

	b, err := resp.ReadLine(reader)
	if err != nil {
		return err
	}
	log.Printf("PSYNC file metadata response: %q\n", string(b))
	size, err := strconv.Atoi(string(b[1 : len(b)-2]))
	if err != nil {
		return err
	}
	buf := make([]byte, size)
	_, err = reader.Read(buf)
	if err != nil && err != io.EOF {
		return err
	}

	log.Printf("PSYNC file response %d bytes\n", len(buf))
	return nil
}

func sendBytes(conn net.Conn, bytes []byte) error {
	_, err := conn.Write(bytes)
	return err
}

func readResponse(reader io.Reader, buf []byte) (string, error) {
	n, err := reader.Read(buf)
	if err != nil {
		return "", err
	}

	return string(buf[:n]), nil
}
