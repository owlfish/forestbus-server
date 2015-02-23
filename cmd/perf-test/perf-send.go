/*
forest-send is a binary command line tool for sending messages to a cluster.  This tool is intended for debugging and testing.

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
	"code.google.com/p/forestbus"
	"encoding/csv"
	"flag"
	"fmt"
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
	var startNumber = flag.Int("start", 1, "Number of clients / batches to start at.")
	var endNumber = flag.Int("end", 1, "Number of clients / batches to end at.")
	var stepSize = flag.Int("step", 1, "Step size to increase the number of clients / batches")
	var numBatches = flag.Int("batches", 1, "Number of batches for each client to send")
	var messageSize = flag.Int("msgSize", 20, "Size in bytes of the messages to be sent")
	var waitForCommit = flag.Bool("wait", true, "Wait for commit on each batch send")
	var enableDebug = flag.Bool("debug", false, "Enable debug logging")
	var msgTemplate = flag.String("msg", "", "Message content to send - will be truncated to messageSize if required.")

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
		fmt.Printf("At least one node name is required.\n")
	}

	if topic == "" {
		fmt.Printf("topic missing.\n")
		flag.PrintDefaults()
		return
	}

	wg := sync.WaitGroup{}

	numClients := *startNumber
	batchSize := *startNumber

	tout, err := os.Create("perf-test-out.csv")
	if err != nil {
		panic("Error opening output file: " + err.Error())
	}
	csvout := csv.NewWriter(tout)
	csvout.Write([]string{"Count", "Clients run 1", "Clients run 2", "Clients run 3", "Clients Avg", "Batch run 1", "Batch run 2", "Batch run 3", "Batch Avg", "Combined run 1", "Combined run 2", "Combined run 3", "Combined Avg"})

	rowResults := make([]string, 0, 13)

	// Create the clients now to avoid having to constantly re-connect.
	clients := make([]*forestbus.Client, 0, *endNumber)
	for i := 0; i < *endNumber; i++ {
		var client *forestbus.Client
		if *enableDebug {
			client = forestbus.NewClient(clusterID, peers, forestbus.ClientEnableDebug())
		} else {
			client = forestbus.NewClient(clusterID, peers)
		}
		clients = append(clients, client)
	}

	for testnumber := *startNumber; testnumber <= *endNumber; testnumber += *stepSize {
		rowResults = append(rowResults, fmt.Sprintf("%v", testnumber))
		for testType := 0; testType < 3; testType++ {
			switch testType {
			case 0:
				numClients = testnumber
				batchSize = 1
			case 1:
				numClients = 1
				batchSize = testnumber
			case 2:
				numClients = testnumber
				batchSize = testnumber
			}

			runRes := make([]float64, 0, 3)

			for testrun := 0; testrun < 3; testrun++ {
				startTime := time.Now()
				for clientCount := 0; clientCount < numClients; clientCount++ {
					wg.Add(1)
					go clientSend(topic, &wg, clients[clientCount], *numBatches, batchSize, *messageSize, *msgTemplate, *waitForCommit)
				}
				wg.Wait()
				endTime := time.Now()

				totalMessages := numClients * *numBatches * batchSize
				totalBytes := float64(totalMessages * *messageSize)
				durationSeconds := endTime.Sub(startTime).Seconds()

				runRes = append(runRes, float64(totalMessages)/durationSeconds)
				rowResults = append(rowResults, fmt.Sprintf("%.f", float64(totalMessages)/durationSeconds))

				fmt.Printf("%v messages (%v) in %v [%v msgs/sec and %v/sec]\n", totalMessages, formatBytes(totalBytes), endTime.Sub(startTime), fmt.Sprintf("%.f", float64(totalMessages)/durationSeconds), formatBytes(float64(totalBytes)/durationSeconds))
			}
			avg := (runRes[0] + runRes[1] + runRes[2]) / 3
			rowResults = append(rowResults, fmt.Sprintf("%.f", avg))
			runRes = runRes[:0]
		}
		csvout.Write(rowResults)
		rowResults = rowResults[:0]
	}
	csvout.Flush()
	tout.Close()
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
