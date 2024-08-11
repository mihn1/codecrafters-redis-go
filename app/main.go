package main

import (
	"flag"
)

func main() {
	port := flag.Int("port", 6379, "Port to listen on")
	dir := flag.String("dir", "/tmp/redis-files", "Directory to store RDB files")
	replicaof := flag.String("replicaof", "", "Replica of host:port")
	dbFileName := flag.String("dbfilename", "dump.rdb", "Name of the RDB file")

	flag.Parse()

	server := NewServer(ServerOptions{
		Port:       *port,
		DbFilename: *dbFileName,
		Dir:        *dir,
		Replicaof:  *replicaof,
	})

	server.Run()
}
