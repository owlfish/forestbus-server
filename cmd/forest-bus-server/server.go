/*
forest-bus-server is the binary command for the server component of Forest Bus.  At least one instance of this is required, although a more common configuration is to have three nodes in order to provide reslience of one node.

Usage flags are:

	-name The hostname and port (e.g. localhost:3000) that the node should listen on.  This specifies the RPC interface port.
	-gob  Alternative gob network name and port for clients, allowing clients to connect over a different physical interface to nodes.
	-http The hostname and port number for running the http interface on (used for ExpVar).
	-cbor The hostname and port number for running the CBOR RPC interface on (used for non-Golang clients).
	-path The path to the working directory for this node.  Should be an initially empty directory.
	-id The Cluster ID that this node will be part of.
*/
package main

import (
	"code.google.com/p/forestbus.server/server"
	"expvar"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
)

import _ "net/http/pprof"

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lmicroseconds)
}

const DEFAULT_PORT_NUMBER = ":3000"

const REQUIRED = "REQUIRED"

func main() {
	// Initialise our configuration from flags

	var nodeName = flag.String("name", REQUIRED, "Node network name and port, e.g. localhost:3000")
	var gobName = flag.String("gob", "", "Alternative gob network name and port for clients, allowing clients to connect over a different physical interface to nodes.")
	var httpName = flag.String("http", "", "Network name and port for the http ExpVar to listen on.")
	var cborName = flag.String("cbor", "", "Network name and port for the CBOR RPC interface to listen on.")
	var nodePath = flag.String("path", REQUIRED, "Node root path for configuration and log files")
	var clusterID = flag.String("id", "", "Cluster ID that this node is part of")

	flag.Parse()

	if flag.Lookup("help") != nil || flag.Lookup("h") != nil {
		flag.PrintDefaults()
		return
	}

	if *nodeName == REQUIRED {
		log.Printf("name missing.\n")
		flag.PrintDefaults()
		return
	}

	if *nodePath == REQUIRED {
		log.Printf("path missing.\n")
		flag.PrintDefaults()
		return
	}

	// Create our server
	serverNode, err := server.NewServerNode(*nodeName, *gobName, *httpName, *cborName, *nodePath, *clusterID)

	if err != nil {
		log.Fatalf("Unable to start server due to errors.\n")
	}

	expvar.Publish("node", expvar.Func(serverNode.ExpVar))

	//dataLog := &memlog.MemoryLog{}

	// Start a listener to handle incoming requests from other peers
	err = serverNode.ListenConnections()
	if err != nil {
		log.Fatalf("Error starting listener: %v\n", err)
	}

	// Setup signal handling to catch interrupts.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		os.Interrupt,
		os.Kill)
	go func() {
		<-sigc
		serverNode.RequestShutdown("Request to terminate process detected.")
	}()

	serverNode.WaitForShutdown()

}
