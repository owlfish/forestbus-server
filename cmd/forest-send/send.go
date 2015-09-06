/*
forest-send is a tool for piping messages from stdin to a Forest Bus server.

Usage flags are:

  -forestbus="REQUIRED": Connection string for Forest Bus cluster in ClusterID#Topic@host:port,host:port... format.
  -debug="false": Enable diagnostic debug
*/
package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/owlfish/forestbus"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

type State int

const (
	RUNNING State = iota
	DRAINING
	COMPLETING
	ABORTING
)

const REQUIRED = "REQUIRED"

func main() {
	// Initialise our configuration from flags
	var fbConnect = flag.String("forestbus", REQUIRED, "Connection string for Forest Bus cluster in ClusterID#Topic@host:port,host:port... format.")
	var enableDebug = flag.Bool("debug", false, "Enable debug logging")

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
		fmt.Printf("At least one node name is required.\n")
	}

	if topic == "" {
		fmt.Printf("topic missing.\n")
		flag.PrintDefaults()
		return
	}

	client := forestbus.NewClient(clusterID, peers)
	// Try up to 7 times (~14s) if there is a leadership change or network glitch
	batcher := forestbus.NewMessageBatcher(client, topic, forestbus.MessageBatcherRetries(forestbus.UNLIMITED_RETRIES))

	// Listen for signals
	exitSignal := make(chan os.Signal, 10)
	signal.Notify(exitSignal, os.Interrupt, os.Kill)

	// Setup the stdin reader
	reader := NewStdInReader()
	go reader.ReadStdIn()

	// Setup the message sender
	sender := NewMsgSender(batcher)
	go sender.Send(reader.MsgChan)
	var state State = RUNNING

	running := true
	if *enableDebug {
		log.Printf("Reading from stdin to send to topic %v\n", topic)
	}
	var sentCount, errCount int
	for running {
		select {
		case <-exitSignal:
			switch state {
			case RUNNING:
				// Ask the reader to stop generating new messages
				fmt.Printf("Exit detected - shutting down\n")
				reader.RequestStop()
				batcher.Close()
			case DRAINING:
				// Time to abort
				fmt.Printf("Abort detected - shutting down\n")
				sender.RequestStop()
				batcher.Close()
				state = ABORTING
			case COMPLETING:
				fmt.Printf("Abort detected - shutting down\n")
				state = ABORTING
			}
		case <-reader.FinishedChan:
			if state == RUNNING {
				state = DRAINING
			}
		case <-sender.FinishedChan:
			// Draining or running or abort has finished - all messages are in the MessageBatcher
			fmt.Printf("Messages sent - completing\n")
			state = COMPLETING
			batcher.Close()
			// Clear any final message stuck in Client
			client.Close()
			// Now we are done
			running = false
		case result := <-sender.ResultChan:
			if sendErr := result.GetError(); sendErr != nil {
				fmt.Printf("Error sending message: %v\n", sendErr)
				// Tell our stdin reader to stop reading and wait for it to confirm
				reader.RequestStop()
				errCount++
			} else {
				sentCount++
			}
		}
	}

	// Wait for all message results to come back
	waitToClear := true
	if (errCount + sentCount) == reader.Count {
		waitToClear = false
	}
	timeout := time.NewTimer(2 * time.Second)
	for waitToClear {
		select {
		case result := <-sender.ResultChan:
			if sendErr := result.GetError(); sendErr != nil {
				errCount++
			} else {
				sentCount++
			}
			if (errCount + sentCount) == reader.Count {
				waitToClear = false
			}
		case <-timeout.C:
			waitToClear = false
		}
	}

	fmt.Printf("Read %v messages, sent %v messages succesfully and %v errors.\n", reader.Count, sentCount, errCount)
}

type MsgSender struct {
	batcher         *forestbus.MessageBatcher
	ResultChan      chan *forestbus.SendResult
	FinishedChan    chan interface{}
	StopRequestChan chan interface{}
}

func NewMsgSender(batcher *forestbus.MessageBatcher) *MsgSender {
	sender := &MsgSender{batcher: batcher}
	sender.ResultChan = make(chan *forestbus.SendResult, 1000)
	sender.FinishedChan = make(chan interface{}, 1)
	sender.StopRequestChan = make(chan interface{}, 1)
	return sender
}

func (sender *MsgSender) Send(MsgChan chan []byte) {
	running := true
	for running {
		select {
		case <-sender.StopRequestChan:
			// Stop sending
			running = false
		case msg, ok := <-MsgChan:
			if ok {
				sender.batcher.AsyncSendMessage(msg, nil, sender.ResultChan, true)
			} else {
				// There are no more messages - so stop looping
				running = false
			}
		}
	}
	// Now we have finished sending, it's safe to abort - let our controller know
	sender.FinishedChan <- struct{}{}
}

func (sender *MsgSender) RequestStop() {
	select {
	case sender.StopRequestChan <- struct{}{}:
	default:
	}
}

type StdInReader struct {
	MsgChan         chan []byte
	FinishedChan    chan interface{}
	StopRequestChan chan interface{}
	Count           int
	// Protection for msgChanStopped
	msgChanStoppedLock *sync.RWMutex
	msgChanStopped     bool
	inputBuffer        *bufio.Reader
}

func NewStdInReader() *StdInReader {
	reader := &StdInReader{}
	reader.MsgChan = make(chan []byte, 100)
	reader.FinishedChan = make(chan interface{}, 1)
	reader.StopRequestChan = make(chan interface{}, 1)
	reader.msgChanStoppedLock = &sync.RWMutex{}
	return reader
}

func (reader *StdInReader) ReadStdIn() {
	// Loop over stdin and read messages
	//reader.inputBuffer = bufio.NewReader(os.Stdin)
	//scanner := bufio.NewScanner(reader.inputBuffer)
	scanner := bufio.NewScanner(os.Stdin)

	running := true
	for running {
		select {
		case <-reader.StopRequestChan:
			running = false
			// No more of stdin will be read at this point
		default:
			if scanner.Scan() {
				reader.msgChanStoppedLock.RLock()
				if !reader.msgChanStopped {
					// If we read something, we must put it in the channel before thinking of finishing.
					msg := scanner.Bytes()
					// Duplicate the message so that it isn't overwritten
					msgToSend := make([]byte, len(msg))
					copy(msgToSend, msg)
					reader.Count++
					// Use a select so that stop during send works.
					select {
					case reader.MsgChan <- msgToSend:
					case <-reader.StopRequestChan:
						running = false
					}
				} else {
					running = false
				}
				reader.msgChanStoppedLock.RUnlock()
			} else {
				// Reading finished
				running = false
			}
		}
	}
	reader.close()
}

func (reader *StdInReader) RequestStop() {
	reader.close()
}

func (reader *StdInReader) close() {
	select {
	case reader.StopRequestChan <- struct{}{}:
	default:
	}
	reader.msgChanStoppedLock.Lock()
	defer reader.msgChanStoppedLock.Unlock()
	if !reader.msgChanStopped {
		close(reader.MsgChan)
		reader.msgChanStopped = true
		reader.FinishedChan <- struct{}{}
	}
}

func PrintUsage() {
	fmt.Fprintf(os.Stderr, "forest-send is a tool for piping messages from stdin to a Forest Bus server or reading messages from a server to stdout.\n\n")
	flag.PrintDefaults()
}
