package server

import (
	"log"
	//"runtime"
	"code.google.com/p/forestbus.server/commitlog"
	"code.google.com/p/forestbus.server/utils"
	"sync"
	"time"
)

// DEFAULT_MAX_BATCH_AGGREGATION is the default maximum number of batches (client requests) to aggregate into a single write
const DEFAULT_MAX_BATCH_AGGREGATION = 1500

// DEFAULT_TRIGGER_TOTAL_AGGREGATED_MESSAGES is the default trigger point beyond which we stop aggegating and do a send to the commit log
// This is set to the maximum number of messages that a leader sends to followers in one go
const DEFAULT_TRIGGER_TOTAL_AGGREGATED_MESSAGES = commitlog.MAX_RETRIEVE_MESSAGE_COUNT

// DEFAULT_AGGREGATION_WINDOW is the default amount of time to wait on additional messages arriving for aggregation prior to writing.
const DEFAULT_AGGREGATION_WINDOW = time.Millisecond * 3

// writeResponse holds the results of a given batches aggregated write.
type writeResponse struct {
	IDs   []int64
	Error error
}

// writeRequest is used to submit the individual batches into the aggregator.  The channel is used to notify the waiting write routine.
type writeRequest struct {
	replyChannel chan writeResponse
	messages     [][]byte
}

/*
QueueWriteAggregator is used to aggregate client messages into larger batches, increasing the through-put of situations where a large number of clients are sending messages.
*/
type QueueWriteAggregator struct {
	sendQueue       chan writeRequest
	shutdownQueue   chan *utils.ShutdownNotifier
	node            *Node
	statLock        *sync.RWMutex
	totalBatches    int
	totalMessages   int
	totalQueueCalls int
}

// NewQueueWriteAggregator is used to create a new instance of the queue aggregator for the given node.
func NewQueueWriteAggregator(node *Node) *QueueWriteAggregator {
	result := &QueueWriteAggregator{}
	result.sendQueue = make(chan writeRequest, DEFAULT_MAX_BATCH_AGGREGATION)
	result.shutdownQueue = make(chan *utils.ShutdownNotifier, 1)
	result.statLock = &sync.RWMutex{}
	result.node = node
	go result.writer()
	return result
}

// Queue turns a batch of messages into a request in the send queue and waits for the aggregator to respond
func (agg *QueueWriteAggregator) Queue(messages [][]byte) ([]int64, error) {
	request := writeRequest{}
	// Make them one message long so that we don't have to context switch so much.
	request.replyChannel = make(chan writeResponse, 1)
	request.messages = messages
	agg.sendQueue <- request
	// Wait for response
	var response writeResponse
	response = <-request.replyChannel
	// Now send back the response
	return response.IDs, response.Error
}

// Shutdown the aggregator.
func (agg *QueueWriteAggregator) Shutdown(notifier *utils.ShutdownNotifier) {
	agg.shutdownQueue <- notifier
}

// writer takes messages from the internal queue and decides when to send them to the commit log.
func (agg *QueueWriteAggregator) writer() {
	// List of messagse to be aggregated
	messagesToBeAggregated := make([]writeRequest, 0, DEFAULT_MAX_BATCH_AGGREGATION)
	totalMessages := 0
	var request writeRequest
	//deadline := time.NewTimer(DEFAULT_AGGREGATION_WINDOW)
	var notifier *utils.ShutdownNotifier
	running := true
	for running {
		// Blocking wait for the first message
		select {
		case notifier = <-agg.shutdownQueue:
			running = false
		case request = <-agg.sendQueue:
			// We have a new request
			messagesToBeAggregated = append(messagesToBeAggregated, request)
			totalMessages += len(request.messages)
			gathering := true
			// Wait for additional requests to arrive.
			time.Sleep(DEFAULT_AGGREGATION_WINDOW)
			//deadline.Reset(DEFAULT_AGGREGATION_WINDOW)
			//runtime.Gosched()
			// Now pull as many requests as possible.  When there are none left or we have reached our limit, send them.
			for gathering {
				select {
				case request = <-agg.sendQueue:
					// We have additional requests, queue them.
					messagesToBeAggregated = append(messagesToBeAggregated, request)
					totalMessages += len(request.messages)
					if totalMessages >= DEFAULT_TRIGGER_TOTAL_AGGREGATED_MESSAGES || len(messagesToBeAggregated) >= DEFAULT_MAX_BATCH_AGGREGATION {
						gathering = false
					}
					// case <-deadline.C:
					// 	// We've waited as long as we can - time to send what we have accumulated.
					// 	gathering = false
				default:
					gathering = false
				}
			}
			agg.sendMessages(messagesToBeAggregated, totalMessages)
			messagesToBeAggregated = messagesToBeAggregated[:0]
			totalMessages = 0
		}
	}
	notifier.ShutdownDone()
}

// sendMessages performs the actual aggregation, the write and the reply
func (agg *QueueWriteAggregator) sendMessages(messagesToBeAggregated []writeRequest, totalMessages int) {
	//log.Printf("Aggregating %v batches into one batch of %v messages\n", len(messagesToBeAggregated), totalMessages)
	aggregatedMessages := make([][]byte, totalMessages)
	batchLengths := make([]int, len(messagesToBeAggregated))
	cumulativeLength := 0
	for i, request := range messagesToBeAggregated {
		length := copy(aggregatedMessages[cumulativeLength:], request.messages)
		batchLengths[i] = length
		cumulativeLength += length
	}

	if cumulativeLength != totalMessages {
		log.Fatalf("Cumulative length of %v did not match total messages %v\n", cumulativeLength, totalMessages)
	}

	// Now try the append.
	IDs, err := agg.node.log.Queue(agg.node.getTerm(), aggregatedMessages)

	// Record some stats
	agg.statLock.Lock()
	agg.totalBatches += len(messagesToBeAggregated)
	agg.totalMessages += totalMessages
	agg.totalQueueCalls++
	agg.statLock.Unlock()

	// Send the results to people.

	idoffset := 0
	if err == nil && len(IDs) == cumulativeLength {
		// We have the results we expect - send them back.
		for i, request := range messagesToBeAggregated {
			request.replyChannel <- writeResponse{IDs: IDs[idoffset : idoffset+batchLengths[i]], Error: err}
		}
		// Tell our peers about the append.
		agg.node.SendLogsToPeers()
	} else {
		// We don't have the IDs we need
		for _, request := range messagesToBeAggregated {
			request.replyChannel <- writeResponse{IDs: nil, Error: err}
		}
	}

}

func (agg *QueueWriteAggregator) ExpVar() interface{} {
	agg.statLock.RLock()
	defer agg.statLock.RUnlock()
	return &struct {
		TotalBatches    int
		TotalMessages   int
		TotalQueueCalls int
	}{TotalBatches: agg.totalBatches, TotalMessages: agg.totalMessages, TotalQueueCalls: agg.totalQueueCalls}
}
