package commitlog

import (
	"code.google.com/p/forestbus.server/memlog"
	"code.google.com/p/forestbus.server/model"
	"runtime"
	"testing"
	"time"
)

// This is declared in MessageCache_test.go
func getMessages(count int) model.Messages {
	msgs := model.EMPTY_MESSAGES
	msgPayloads := make([][]byte, count)
	for i, _ := range msgPayloads {
		msgPayloads[i] = []byte{byte(i + 1)}
		additionalMsgs := model.MessagesFromClientData(msgPayloads[i : i+1])
		additionalMsgs.SetMessageTerm(int64(i + 1))
		msgs, _ = msgs.Join(additionalMsgs)
	}

	return msgs
}

func getClientMessages(count int) [][]byte {
	msgs := make([][]byte, count)
	for i, _ := range msgs {
		msg := make([]byte, 5)
		msg[0] = byte(i)
		msgs[i] = msg
	}
	return msgs
}

func getCommitLog() *CommitLog {
	clog := &CommitLog{}
	store := &memlog.MemoryLogStorage{}
	clog.SetupLog("test", store)
	return clog
}

func TestGetEmptyLog(t *testing.T) {
	clog := getCommitLog()
	clientMsgs, nextID, err := clog.Get(1, 1, false)
	if err != nil {
		t.Errorf("Error getting messages when empty: %v\n", err)
	}
	if len(clientMsgs) != 0 {
		t.Errorf("Client messages returned unexpectedly")
	}
	if nextID != 0 {
		t.Errorf("Client messages didn't return 0 as the next ID")
	}

	clientMsgs, nextID, err = clog.Get(2, 1, false)
	if err != nil {
		t.Errorf("Error getting messages when empty: %v\n", err)
	}
	if len(clientMsgs) != 0 {
		t.Errorf("Client messages returned unexpectedly")
	}
	if nextID != 0 {
		t.Errorf("Client messages didn't return 0 as the next ID: %v", nextID)
	}
}

func TestRetrieveEmptyLog(t *testing.T) {
	clog := getCommitLog()
	previousIndex, previousTerm, modelMsgs, err := clog.Retrieve(1)
	if err != nil {
		t.Errorf("Error retrieving messages when empty: %v\n", err)
	}
	if modelMsgs.GetCount() != 0 {
		t.Errorf("Model messages returned unexpectedly")
	}
	if previousIndex != 0 {
		t.Errorf("Previous index not returned as 0")
	}
	if previousTerm != 0 {
		t.Errorf("Previous term not returned as 0")
	}
}

func TestAppendMsgsLog(t *testing.T) {
	clog := getCommitLog()
	msgs := getMessages(10)

	// First append should work
	newmsgs, _ := msgs.Slice(0, 1)
	nextIndex, previousMatch, err := clog.Append(newmsgs, 0, 0, 100)
	if err != nil {
		t.Errorf("Error appending single message: %v", err)
	}

	if nextIndex != 101 {
		t.Errorf("Next index should have been two, got: %v", nextIndex)
	}

	if previousMatch != true {
		t.Errorf("Previous match was not true")
	}
	// Second append should fail to find a previous match with this index and term.
	newmsgs, _ = msgs.Slice(1, 2)
	nextIndex, previousMatch, err = clog.Append(newmsgs, 0, 0, 100)

	if err != nil {
		t.Errorf("Error appending single message: %v", err)
	}

	if nextIndex != 101 {
		t.Errorf("Next index should have been 101, got: %v", nextIndex)
	}

	if previousMatch != false {
		t.Errorf("Previous match was not false")
	}
	index, _, err := clog.LastLogEntryInfo()
	if err != nil {
		t.Error("Error")
	}
	if index != 100 {
		t.Errorf("Last log entry was not 100")
	}

	// Third append should work and find the previous match with this index and term.
	newmsgs, _ = msgs.Slice(1, 2)
	nextIndex, previousMatch, err = clog.Append(newmsgs, 100, 1, 100)
	if err != nil {
		t.Errorf("Error appending single message: %v", err)
	}

	if nextIndex != 102 {
		t.Errorf("Next index should have been three, got: %v", nextIndex)
	}

	if previousMatch != true {
		t.Errorf("Previous match was not true")
	}
	index, _, err = clog.LastLogEntryInfo()
	if err != nil {
		t.Error("Error")
	}
	if index != 101 {
		t.Errorf("Last log entry was not 102: %v", index)
	}

	// We now have two messages stored.  Send another two, with an overlap of one
	newmsgs, _ = msgs.Slice(1, 3)
	nextIndex, previousMatch, err = clog.Append(newmsgs, 100, 1, 100)
	if err != nil {
		t.Errorf("Error appending two messages: %v", err)
	}

	if nextIndex != 103 {
		t.Errorf("Next index should have been four, got: %v", nextIndex)
	}

	if previousMatch != true {
		t.Errorf("Previous match was not true")
	}
	index, _, err = clog.LastLogEntryInfo()
	if err != nil {
		t.Error("Error")
	}
	if index != 102 {
		t.Errorf("Last log entry was not 102, got %v", index)
	}

	// We now have three messages stored.  Send another five, with an overlap of three
	newmsgs, _ = msgs.Slice(1, 5)
	nextIndex, previousMatch, err = clog.Append(newmsgs, 100, 1, 100)
	if err != nil {
		t.Errorf("Error appending five messages: %v", err)
	}

	if nextIndex != 105 {
		t.Errorf("Next index should have been 105, got: %v", nextIndex)
	}

	if previousMatch != true {
		t.Errorf("Previous match was not true")
	}
	index, _, err = clog.LastLogEntryInfo()
	if err != nil {
		t.Error("Error")
	}
	if index != 104 {
		t.Errorf("Last log entry was not 104, got %v", index)
	}

	// We now have five messages stored.  Send another five, with an overlap of one
	// Should truncate the log as the terms of stored message 5 does not match that passed in
	// Result should be 9 messages stored
	newmsgs, _ = msgs.Slice(0, 5)
	nextIndex, previousMatch, err = clog.Append(newmsgs, 103, 4, 100)
	if err != nil {
		t.Errorf("Error appending five messages: %v", err)
	}

	if nextIndex != 110 {
		t.Errorf("Next index should have been six, got: %v", nextIndex)
	}

	if previousMatch != true {
		t.Errorf("Previous match was not true")
	}
	index, _, err = clog.LastLogEntryInfo()
	if err != nil {
		t.Error("Error")
	}
	if index != 109 {
		t.Errorf("Last log entry was not 109, got %v", index)
	}

	// Finally do another append where previous term doesn't match, triggering no match.
	// Second append should fail to find a previous match with this index and term.
	newmsgs, _ = msgs.Slice(5, 9)
	nextIndex, previousMatch, err = clog.Append(newmsgs, 109, 1, 100)

	if err != nil {
		t.Errorf("Error appending message: %v", err)
	}

	if nextIndex != 110 {
		t.Errorf("Next index should have been 10, got: %v", nextIndex)
	}

	if previousMatch != false {
		t.Errorf("Previous match was not false")
	}
	index, _, err = clog.LastLogEntryInfo()
	if err != nil {
		t.Error("Error")
	}
	if index != 109 {
		t.Errorf("Last log entry was not 9")
	}

}

func TestAppendAndGetMsgsLog(t *testing.T) {
	clog := getCommitLog()
	msgs := getMessages(10)
	// Append five messages
	newmsgs, _ := msgs.Slice(0, 5)
	nextIndex, previousMatch, err := clog.Append(newmsgs, 0, 0, 100)
	if err != nil {
		t.Errorf("Error appending single message: %v", err)
	}

	if nextIndex != 105 {
		t.Errorf("Next index should have been 105, got: %v", nextIndex)
	}

	if previousMatch != true {
		t.Errorf("Previous match was not true")
	}

	index, _, err := clog.LastLogEntryInfo()
	if err != nil {
		t.Error("Error")
	}
	if index != 104 {
		t.Errorf("Last log entry was not 5")
	}

	clientMsgs, nextID, err := clog.Get(100, 1, false)

	if err != nil {
		t.Errorf("Error in Get: %v\n", err)
	}

	if len(clientMsgs) != 0 {
		t.Errorf("Expected no messages from get, recieved: %v", clientMsgs)
	}

	if nextID != 0 {
		t.Errorf("NextID should be 0, got %v\n", nextID)
	}

	// Now commit to 103
	clog.Commit(103)

	clientMsgs, nextID, err = clog.Get(100, 4, false)

	if err != nil {
		t.Errorf("Error in Get: %v\n", err)
	}

	if len(clientMsgs) != 4 {
		t.Errorf("Expected 4 messages from get, recieved: %v", clientMsgs)
	}

	if nextID != 104 {
		t.Errorf("NextID should be 104, got %v\n", nextID)
	}

	// Now commit to 104
	clog.Commit(104)

	clientMsgs, nextID, err = clog.Get(104, 1, false)

	if err != nil {
		t.Errorf("Error in Get: %v\n", err)
	}

	if len(clientMsgs) != 1 {
		t.Errorf("Expected 1 messages from get, recieved: %v", clientMsgs)
	}

	if nextID != 105 {
		t.Errorf("NextID should be 105, got %v\n", nextID)
	}
}

func TestQueueMsgs(t *testing.T) {
	clog := getCommitLog()
	msgs := getClientMessages(10)
	// Append 0 messages
	IDs, err := clog.Queue(1, msgs[0:0])
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
	if len(IDs) != 0 {
		t.Errorf("Got IDs - expected nothing")
	}
	// Append 5 messages
	IDs, err = clog.Queue(1, msgs[0:5])
	if err != nil {
		t.Errorf("Expected not error")
	}
	if len(IDs) != 5 {
		t.Errorf("Expected 5 IDs, got %v", IDs)
	}
	if IDs[0] != 1 {
		t.Errorf("IDs did not start at 1, got: %v", IDs)
	}

	// Append another 2
	IDs, err = clog.Queue(1, msgs[0:2])
	if err != nil {
		t.Errorf("Expected not error")
	}
	if len(IDs) != 2 {
		t.Errorf("Expected 5 IDs, got %v", IDs)
	}
	if IDs[0] != 6 {
		t.Errorf("IDs did not start at 6, got: %v", IDs)
	}
}

func TestQueueAndRetrieveMsgs(t *testing.T) {
	clog := getCommitLog()
	msgs := getClientMessages(10)
	// Append 0 messages
	IDs, err := clog.Queue(1, msgs[0:0])
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
	if len(IDs) != 0 {
		t.Errorf("Got IDs - expected nothing")
	}
	// Append 5 messages
	IDs, err = clog.Queue(1, msgs[0:5])
	if err != nil {
		t.Errorf("Expected not error")
	}
	if len(IDs) != 5 {
		t.Errorf("Expected 5 IDs, got %v", IDs)
	}
	if IDs[0] != 1 {
		t.Errorf("IDs did not start at 1, got: %v", IDs)
	}

	// Now retrieve the 5 messagse
	previousIndex, previousTerm, savedMsgs, err := clog.Retrieve(1)
	if err != nil {
		t.Error("Error")
	}
	if savedMsgs.GetCount() != 5 {
		t.Errorf("Expected 5 messages, got: %v", savedMsgs)
	}
	if previousTerm != 0 {
		t.Errorf("Got non zero previous term: %v", previousTerm)
	}
	if previousIndex != 0 {
		t.Errorf("Got non zero previous index: %v", previousIndex)
	}

	// Append another 2
	IDs, err = clog.Queue(2, msgs[0:2])
	if err != nil {
		t.Errorf("Expected not error")
	}
	if len(IDs) != 2 {
		t.Errorf("Expected 5 IDs, got %v", IDs)
	}
	if IDs[0] != 6 {
		t.Errorf("IDs did not start at 6, got: %v", IDs)
	}

	// Now retrieve the 2 messagse
	previousIndex, previousTerm, savedMsgs, err = clog.Retrieve(6)
	if err != nil {
		t.Error("Error")
	}
	if savedMsgs.GetCount() != 2 {
		t.Errorf("Expected 2 messages, got: %v", savedMsgs)
	}
	if previousTerm != 1 {
		t.Errorf("Got previous term: %v", previousTerm)
	}
	if previousIndex != 5 {
		t.Errorf("Got previous index: %v", previousIndex)
	}
}

func TestAppendAndGetWithWaitMsgsLog(t *testing.T) {
	clog := getCommitLog()
	msgs := getMessages(10)
	timeOfGetChan := make(chan time.Time, 1)

	// Spawn a get goroutine
	go func() {
		clientMsgs, nextID, err := clog.Get(4, 1, true)
		if len(clientMsgs) != 1 {
			t.Errorf("Expected one message, got %v", clientMsgs)
		}
		if nextID != 5 {
			t.Errorf("Expected Next ID of 5, got %v", nextID)
		}
		if err != nil {
			t.Errorf("Error in get: %v", err)
		}
		timeOfGetChan <- time.Now()
	}()
	// Yield so that our read routine has a chance.
	runtime.Gosched()

	// Append five messages
	startOfAppend := time.Now()
	newmsgs, _ := msgs.Slice(0, 5)
	nextIndex, previousMatch, err := clog.Append(newmsgs, 0, 0, 1)
	if err != nil {
		t.Errorf("Error appending single message: %v", err)
	}

	if nextIndex != 6 {
		t.Errorf("Next index should have been six, got: %v", nextIndex)
	}

	if previousMatch != true {
		t.Errorf("Previous match was not true")
	}

	index, _, err := clog.LastLogEntryInfo()
	if err != nil {
		t.Error("Error")
	}
	if index != 5 {
		t.Errorf("Last log entry was not 5")
	}

	// Now commit to 3
	startOfCommit3 := time.Now()
	clog.Commit(3)

	// Now commit to 4
	startOfCommit4 := time.Now()
	clog.Commit(4)

	// Check that the goroutine fired when we expect it to.
	result := <-timeOfGetChan

	if result.Before(startOfAppend) {
		t.Error("Get returned before append.")
	}
	if result.Before(startOfCommit3) {
		t.Error("Get returned before startOfCommit3.")
	}
	if result.Before(startOfCommit4) {
		t.Error("Get returned before startOfCommit4.")
	}

}

func BenchmarkQueueSingleMessage(b *testing.B) {
	clog := getCommitLog()
	msgs := getClientMessages(1)
	for n := 0; n < b.N; n++ {
		clog.Queue(1, msgs)
	}
}

func BenchmarkAppendSingleMessage(b *testing.B) {
	clog := getCommitLog()
	msgs := getMessages(1)
	var lastIndex int64 = 0
	lastTerm, _ := msgs.GetMessageTerm(0)
	for n := 0; n < b.N; n++ {
		nextIndex, previousMatch, err := clog.Append(msgs, lastIndex, lastTerm, 100)
		if err != nil {
			b.Fatalf("Error during benchmark: %v", err)
		}
		if !previousMatch {
			b.Fatalf("Previous match not true")
		}
		lastIndex = nextIndex - 1
	}
}
