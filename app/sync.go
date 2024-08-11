package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/resp"
)

func syncWithMaster(s *Server) error {
	err := handshake(s)
	return err
}

func handshake(s *Server) error {
	masterAddr := fmt.Sprintf("%s:%d", s.slave.masterHost, s.slave.masterPort)
	log.Println("Syncing...", masterAddr)
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		log.Fatalf("Error connecting to master: %v", err)
		return err
	}
	defer conn.Close()

	log.Println("Sending PING to master")
	err = sendPing(s, conn)
	if err != nil {
		log.Fatalf("Error sending PING: %v", err)
		return err
	}

	log.Println("Sending replication config to master")
	err = sendReplConfig(s, conn)
	if err != nil {
		log.Fatalf("Error sending replication config: %v", err)
		return err
	}

	err = sendPSYNC(s, conn)
	if err != nil {
		log.Fatalf("Error sending PSYNC: %v", err)
		return err
	}

	return err
}

func sendPing(s *Server, conn net.Conn) error {
	err := sendMaster(conn, resp.EncodeArrayBulkStrings([]string{"PING"}))
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
	err := sendMaster(conn, message)
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
	err = sendMaster(conn, message)
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

func sendPSYNC(s *Server, conn net.Conn) error {
	err := sendMaster(conn, resp.EncodeArrayBulkStrings([]string{"PSYNC", "?", "-1"}))
	if err != nil {
		return err
	}

	// TODO: validate response
	// err = decodeCheckSimpleString(conn, "FULLRESYNC <REPL_ID> 0")
	return nil
}

func sendMaster(conn net.Conn, message string) error {
	_, err := conn.Write([]byte(message))
	return err
}

func readResponse(reader io.Reader, buf []byte) (string, error) {
	n, err := reader.Read(buf)
	if err != nil {
		return "", err
	}

	return string(buf[:n]), nil
}
