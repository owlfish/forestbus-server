/*
forest-test-send is a binary command line tool for sending test messages to a cluster.  This tool is intended for debugging and testing.

Usage flags are:

  -batchSize=1: Number of messages in each batch sent
  -batches=1: Number of batches for each client to send
  -clients=1: Number of clients to run simultaneously
  -debug=false: Enable debug logging
  -id="": Cluster ID for the nodes
  -msg="": Message content to send - will be truncated to messageSize if required.
  -msgSize=20: Size in bytes of the messages to be sent
  -forestbus="REQUIRED": Connection string for Forest Bus cluster in ClusterID#Topic@host:port,host:port... format.
*/
package main

import (
	"flag"
	"fmt"
	"github.com/owlfish/forestbus"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

const DEFAULT_PORT_NUMBER = ":3000"

const REQUIRED = "REQUIRED"

func main() {
	// Initialise our configuration from flags
	var fbConnect = flag.String("forestbus", REQUIRED, "Connection string for Forest Bus cluster in ClusterID#Topic@host:port,host:port... format.")
	var numClients = flag.Int("clients", 1, "Number of clients to run simultaneously")
	var numBatches = flag.Int("batches", 1, "Number of batches for each client to send")
	var batchSize = flag.Int("batchSize", 1, "Number of messages in each batch sent")
	var messageSize = flag.Int("msgSize", 20, "Size in bytes of the messages to be sent")
	var waitForCommit = flag.Bool("wait", true, "Wait for commit on each batch send")
	var enableDebug = flag.Bool("debug", false, "Enable debug logging")
	var msgTemplate = flag.String("msg", "", "Message content to send - will be truncated to messageSize if required.")

	flag.Usage = PrintUsage

	flag.Parse()

	if flag.Lookup("help") != nil || flag.Lookup("h") != nil {
		flag.PrintDefaults()
		return
	}

	if *fbConnect == REQUIRED {
		fmt.Printf("forestbus connection string missing.\n")
		flag.PrintDefaults()
		return
	}

	clusterID, topic, peers := forestbus.ParseConnectionString(*fbConnect)

	if len(peers) == 0 {
		PrintUsage()
		fmt.Printf("\nError: At least one node name is required.\n")
		return
	}

	if topic == "" {
		PrintUsage()
		fmt.Printf("\nError: -topic is missing.\n")
		return
	}

	wg := sync.WaitGroup{}

	startTime := time.Now()
	for clientCount := 0; clientCount < *numClients; clientCount++ {
		wg.Add(1)
		var client *forestbus.Client
		if *enableDebug {
			client = forestbus.NewClient(clusterID, peers, forestbus.ClientEnableDebug())
		} else {
			client = forestbus.NewClient(clusterID, peers)
		}
		go clientSend(topic, &wg, client, *numBatches, *batchSize, *messageSize, *msgTemplate, *waitForCommit)
	}
	wg.Wait()
	endTime := time.Now()

	totalMessages := *numClients * *numBatches * *batchSize
	totalBytes := float64(totalMessages * *messageSize)
	durationSeconds := endTime.Sub(startTime).Seconds()
	fmt.Printf("%v messages (%v) in %v [%v msgs/sec and %v/sec]\n", totalMessages, formatBytes(totalBytes), endTime.Sub(startTime), fmt.Sprintf("%.f", float64(totalMessages)/durationSeconds), formatBytes(float64(totalBytes)/durationSeconds))
	log.Printf("Done\n")
}

func clientSend(topic string, wg *sync.WaitGroup, client *forestbus.Client, numBatches int, batchSize int, messageSize int, msgTemplate string, waitForCommit bool) {
	count := 0
	var byteMsgTemplate []byte
	if msgTemplate != "" {
		if messageSize < len(msgTemplate) {
			byteMsgTemplate = []byte(msgTemplate[:messageSize])
		} else {
			byteMsgTemplate = []byte(msgTemplate)
		}
	}
	for batch := 0; batch < numBatches; batch++ {
		msgs := make([][]byte, 0, batchSize)
		for batchContent := 0; batchContent < batchSize; batchContent++ {
			msg := make([]byte, messageSize)
			if msgTemplate != "" {
				msgs = append(msgs, byteMsgTemplate)
			} else {
				if messageSize >= len(time.StampMilli) {
					copy(msg, []byte(time.Now().Format(time.StampMilli)))
				}
				msgs = append(msgs, msg)
			}
		}
		// Send the batch
		_, err := client.SendMessages(topic, msgs, waitForCommit)
		if err != nil {
			fmt.Printf("Client experienced error sending message to cluster: %v after %v messages\n", err, count)
			wg.Done()
			return
		}
		count++
	}
	wg.Done()
}

func formatBytes(size float64) string {
	var units string
	var strSize float64
	if size > 1024*1024*1024 {
		strSize = size / (1024 * 1024 * 1024)
		units = "GB"
	} else if size > 1024*1024 {
		strSize = size / (1024 * 1024)
		units = "MB"
	} else if size > 1024 {
		strSize = size / 1024
		units = "KB"
	} else {
		strSize = size
		units = "B"
	}
	return fmt.Sprintf("%.2f "+units, strSize)
}

func PrintUsage() {
	fmt.Fprintf(os.Stderr, "forest-test-send is a tool for sending test messages to a Forest Bus cluster.\n\n")
	flag.PrintDefaults()
}
