/*
forest-get is a binary command line tool for retrieving messages from a cluster.  This tool is intended for debugging and testing.

  -debug=false: Enable debug logging
  -index=1: Index to start reading messages from
  -forestbus="REQUIRED": Connection string for Forest Bus cluster in ClusterID#Topic@host:port,host:port... format.
  -quantity=1: Number of messages to retrieve
  -tail=false: Keeps reading all available messages, implies -wait=true
  -wait=false: Wait for messages to be available
  -info=false: Print out message IDs and topic information
*/
package main

import (
	"code.google.com/p/forestbus"
	"flag"
	"fmt"
	"os"
	"runtime"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func Print(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg, args...)
}

const DEFAULT_PORT_NUMBER = ":3000"

const REQUIRED = "REQUIRED"

var NEWLINE []byte = []byte("\n")

func main() {
	// Initialise our configuration from flags

	var fbConnect = flag.String("forestbus", REQUIRED, "Connection string for Forest Bus cluster in ClusterID#Topic@host:port,host:port... format.")
	var startIndex = flag.Int64("index", 1, "Index to start reading messages from")
	var quantity = flag.Int("quantity", 1, "Target number of messages to retrieve. A larger or smaller number may be returned")
	var waitForMessages = flag.Bool("wait", false, "Wait for messages to be available")
	var enableDebug = flag.Bool("debug", false, "Enable debug logging")
	var tail = flag.Bool("tail", false, "Keeps reading all available messages, implies -wait=true")

	flag.Parse()

	if flag.Lookup("help") != nil || flag.Lookup("h") != nil {
		flag.PrintDefaults()
		return
	}

	if *fbConnect == REQUIRED {
		Print("forestbus connection string missing.\n")
		flag.PrintDefaults()
		return
	}

	clusterID, topic, peers := forestbus.ParseConnectionString(*fbConnect)

	if len(peers) == 0 {
		Print("At least one node name is required.\n")
	}

	if topic == "" {
		fmt.Printf("Topic is missing.\n")
		flag.PrintDefaults()
		return
	}

	var client *forestbus.Client

	if *enableDebug {
		client = forestbus.NewClient(clusterID, peers, forestbus.ClientEnableDebug())
	} else {
		client = forestbus.NewClient(clusterID, peers)
	}

	maxMsgID, err := client.GetTopicMaxAvailableIndex(topic)
	if err != nil {
		Print("Error determining maximum available message ID: %v\n", err)
		return
	}

	if maxMsgID == 0 {
		Print("Commit index is zero due to a full cluster restart.)\n")
		return
	}

	if *tail {
		*waitForMessages = true
	}

	looping := true
	recievedCount := 0
	firstIndex := *startIndex

	for looping {
		msgs, nextID, err := client.GetMessages(topic, *startIndex, *quantity, *waitForMessages)

		if err != nil {
			Print("Error getting messages: %v\n", err)
			return
		}

		if len(msgs) == 0 {
			if nextID == 0 {
				Print("Commit index is zero due to a full cluster restart.)\n")
				return
			} else if nextID == (*startIndex + 1) {
				// Nothing came back - are we planning to wait for more?
				if !*waitForMessages {
					looping = false
				}
			} else if nextID == *startIndex {
				// Index 1 not available due to clean up
				firstIndex = nextID
			}
		}
		for _, msg := range msgs {
			os.Stdout.Write(msg)
			os.Stdout.Write(NEWLINE)
			recievedCount++
			if recievedCount == *quantity && !*tail {
				looping = false
				break
			}
		}
		*startIndex = nextID
	}

	maxMsgID, err = client.GetTopicMaxAvailableIndex(topic)
	if err != nil {
		Print("Error determining maximum available message ID: %v\n", err)
		return
	}

	Print("Recieved %v messages from index %v\n", recievedCount, firstIndex)
	Print("Maximum available message index: %v\n", maxMsgID)
}
