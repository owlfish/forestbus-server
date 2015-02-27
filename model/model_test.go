package model

import (
	"bytes"
	"io"
	"testing"
)

func TestMessagesFromClientData(t *testing.T) {
	emptyData := make([][]byte, 0)
	msgs := MessagesFromClientData(emptyData)
	if msgs.GetCount() != 0 {
		t.Errorf("Expected no messages, found %v\n", msgs.GetCount())
	}

	var data [][]byte
	msg1 := []byte{1, 2, 3}
	msg2 := []byte{4, 5}

	data = append(data, msg1)
	msgs1 := MessagesFromClientData(data)

	if msgs1.GetCount() != 1 {
		t.Errorf("Expected no messages, found %v\n", msgs.GetCount())
	}

	retrievedData, err := msgs1.Payloads()
	if err != nil {
		t.Errorf("Error getting payloads: %v\n", err)
	}
	if len(retrievedData) != 1 {
		t.Errorf("Unexpected payloads: %v\n", retrievedData)
	}
	if len(retrievedData[0]) != 3 {
		t.Errorf("Unexpected payloads: %v\n", retrievedData)
	}
	if retrievedData[0][0] != 1 {
		t.Errorf("Data incorrect")
	}
	if retrievedData[0][2] != 3 {
		t.Errorf("Data incorrect")
	}

	data = append(data, msg2)
	msgs2 := MessagesFromClientData(data)

	retrievedData, err = msgs2.Payloads()
	if err != nil {
		t.Errorf("Error getting payloads: %v\n", err)
	}
	if len(retrievedData) != 2 {
		t.Errorf("Unexpected payloads: %v\n", retrievedData)
	}
	if len(retrievedData[0]) != 3 {
		t.Errorf("Unexpected payloads: %v\n", retrievedData)
	}
	if retrievedData[0][0] != 1 {
		t.Errorf("Data incorrect")
	}
	if retrievedData[0][2] != 3 {
		t.Errorf("Data incorrect")
	}
	if len(retrievedData[1]) != 2 {
		t.Errorf("Unexpected payloads: %v\n", retrievedData)
	}
	if retrievedData[1][0] != 4 {
		t.Errorf("Data incorrect")
	}
	if retrievedData[1][1] != 5 {
		t.Errorf("Data incorrect")
	}

}

func TestMessageTerms(t *testing.T) {
	var data [][]byte
	msg1 := []byte{1, 2, 3}
	msg2 := []byte{4, 5}

	data = append(data, msg1)
	data = append(data, msg2)
	msgs1 := MessagesFromClientData(data)

	term, err := msgs1.GetMessageTerm(0)
	if term != 0 {
		t.Error("Term not set correctly.")
	}
	if err != nil {
		t.Errorf("Error in getmessage term: %v\n", err)
	}

	msgs1.SetMessageTerm(34)

	term, err = msgs1.GetMessageTerm(0)
	if term != 34 {
		t.Error("Term not set correctly.")
	}
	if err != nil {
		t.Errorf("Error in getmessage term: %v\n", err)
	}

	term, err = msgs1.GetMessageTerm(1)
	if term != 34 {
		t.Error("Term not set correctly.")
	}
	if err != nil {
		t.Errorf("Error in getmessage term: %v\n", err)
	}

}

func TestAppendMessages(t *testing.T) {
	var data, data2 [][]byte
	msg1 := []byte{1, 2, 3}
	msg2 := []byte{4, 5}

	data = append(data, msg1)
	data2 = append(data2, msg2)
	msgs1 := MessagesFromClientData(data)
	msgs2 := MessagesFromClientData(data2)

	if msgs1.GetCount() != 1 {
		t.Errorf("Error - should be only one message.")
	}

	newMsgs, err := msgs1.Join(msgs2)
	if err != nil {
		t.Fatalf("Error from join: %v\n", err)
	}

	if newMsgs.GetCount() != 2 {
		t.Errorf("Error - should be two messages.")
	}

	//fmt.Printf("Msg1: %v\nMsg2: %v\nMessage: %v\n", msgs1, msgs2, newMsgs)
	retrievedData, err := newMsgs.Payloads()
	if err != nil {
		t.Errorf("Error getting payloads: %v\n", err)
	}
	if len(retrievedData) != 2 {
		t.Errorf("Unexpected payloads: %v\n", retrievedData)
	}
	if retrievedData[0][0] != 1 || retrievedData[0][2] != 3 || retrievedData[1][0] != 4 || retrievedData[1][1] != 5 {
		t.Errorf("Unexpected payloads: %v\n", retrievedData)
	}
}

func TestReadingMessages(t *testing.T) {
	buffer := &bytes.Buffer{}
	// Empty read
	emptyMsgs, n, err := MessagesFromReader(buffer, 1000)
	if err != io.EOF {
		t.Errorf("Error reading from empty buffer: %v\n", err)
	}
	if emptyMsgs.GetCount() != 0 {
		t.Errorf("Non zero count from empty buffer\n")
	}

	buffer = &bytes.Buffer{}
	// Buffer - no valid messages, short read.
	buffer.Write([]byte{0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	emptyMsgs, n, err = MessagesFromReader(buffer, MESSAGE_OVERHEAD)
	if err != nil {
		t.Errorf("Error reading from empty buffer: %v\n", err)
	}
	if emptyMsgs.GetCount() != 0 {
		t.Errorf("Non zero count from empty buffer\n")
	}

	buffer = &bytes.Buffer{}
	buffer.Write([]byte{0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	// No vaild messages, long read.
	emptyMsgs, n, err = MessagesFromReader(buffer, 1000)
	if err != nil {
		t.Errorf("Error reading from empty buffer: %v\n", err)
	}
	if emptyMsgs.GetCount() != 0 {
		t.Errorf("Non zero count from empty buffer\n")
	}

	// Two valid messages
	buffer = &bytes.Buffer{}
	var data [][]byte
	msg1 := []byte{1, 2, 3}
	msg2 := []byte{4, 5}

	data = append(data, msg1)
	data = append(data, msg2)

	msgs1 := MessagesFromClientData(data)
	n, err = msgs1.Write(buffer)
	if n != 5+2*MESSAGE_OVERHEAD {
		t.Errorf("Unexpected length :%v\n", n)
	}

	twoMsgs, n, err := MessagesFromReader(buffer, 1000)
	if err != nil {
		t.Errorf("Error reading from empty buffer: %v\n", err)
	}
	if twoMsgs.GetCount() != 2 {
		t.Errorf("Two messages not read from buffer\n")
	}

	// Get them an valid the round trip.
	retrievedData, err := twoMsgs.Payloads()
	if err != nil {
		t.Errorf("Error getting payloads: %v\n", err)
	}
	if len(retrievedData) != 2 {
		t.Errorf("Unexpected payloads: %v\n", retrievedData)
	}
	if len(retrievedData[0]) != 3 {
		t.Errorf("Unexpected payloads: %v\n", retrievedData)
	}
	if retrievedData[0][2] != 3 {
		t.Errorf("Unexpected payloads: %v\n", retrievedData)
	}
	if retrievedData[1][1] != 5 {
		t.Errorf("Unexpected payloads: %v\n", retrievedData)
	}

	// Two messages, one broken CRC
	buffer = &bytes.Buffer{}

	msgs1 = MessagesFromClientData(data)
	// Mess around with the data
	msgs1.Data[len(msgs1.Data)-1] = 100

	n, err = msgs1.Write(buffer)
	if n != 5+2*MESSAGE_OVERHEAD {
		t.Errorf("Unexpected length :%v\n", n)
	}

	twoMsgs, n, err = MessagesFromReader(buffer, 1000)
	if err != ERR_CRC_MISMATCH {
		t.Errorf("Error reading from empty buffer didn't catch CRC failure.")
	}

	// Two messages, truncated stream
	buffer = &bytes.Buffer{}

	msgs1 = MessagesFromClientData(data)
	n, err = msgs1.Write(buffer)
	if n != 5+2*MESSAGE_OVERHEAD {
		t.Errorf("Unexpected length :%v\n", n)
	}

	// Mess with the buffer
	buffer.Truncate(n - 1)

	twoMsgs, n, err = MessagesFromReader(buffer, 1000)
	if err != nil {
		t.Errorf("Error reading from buffer: %v\n", err)
	}
	if twoMsgs.GetCount() != 1 {
		t.Errorf("One message expected.")
	}

}

func TestSliceMessages(t *testing.T) {
	var data [][]byte
	msg1 := []byte{1, 2, 3}
	msg2 := []byte{4, 5}

	data = append(data, msg1)
	data = append(data, msg2)
	msgs1 := MessagesFromClientData(data)

	if msgs1.GetCount() != 2 {
		t.Errorf("Error - should be only one message.")
	}

	// Slice into the same size
	msgs2, err := msgs1.Slice(0, 2)
	if err != nil {
		t.Fatalf("Error from slice: %v\n", err)
	}
	if msgs2.GetCount() != 2 {
		t.Errorf("Message count was not 2 - was %v.  Original msgs %v, new is %v\n.", msgs2.GetCount(), msgs1, msgs2)
	}

	// Slice into zero
	msgsZero, err := msgs1.Slice(0, 0)
	if err != nil {
		t.Fatalf("Error from slice: %v\n", err)
	}
	if msgsZero.GetCount() != 0 {
		t.Errorf("Message count was not 0: %v", msgsZero.GetCount())
	}

	// Slice into one
	msgsOne, err := msgs1.Slice(0, 1)
	if err != nil {
		t.Fatalf("Error from slice: %v\n", err)
	}
	if msgsOne.GetCount() != 1 {
		t.Error("Message count was not 1.")
	}
	result, err := msgsOne.Payloads()
	if result[0][0] != 1 || result[0][2] != 3 {
		t.Errorf("Unexpected result: %v", result)
	}

	// Slice the second message
	msgsOne, err = msgs1.Slice(1, 2)
	if err != nil {
		t.Fatalf("Error from slice: %v\n", err)
	}
	if msgsOne.GetCount() != 1 {
		t.Error("Message count was not 1.")
	}
	result, err = msgsOne.Payloads()
	if result[0][0] != 4 || result[0][1] != 5 {
		t.Errorf("Unexpected result: %v", result)
	}

}

func TestForceReadOneMessage(t *testing.T) {
	// One valid messages
	buffer := &bytes.Buffer{}
	var data [][]byte
	msg1 := []byte{1, 2, 3, 4, 5}

	data = append(data, msg1)

	msgs1 := MessagesFromClientData(data)
	n, err := msgs1.Write(buffer)
	if n != 5+MESSAGE_OVERHEAD {
		t.Errorf("Unexpected length :%v\n", n)
	}

	oneMessage, n, err := MessagesFromReader(buffer, MESSAGE_OVERHEAD+1)
	if err != nil {
		t.Errorf("Error reading from empty buffer: %v\n", err)
	}
	if oneMessage.GetCount() != 1 {
		t.Errorf("One messages not read from buffer\n")
	}
}
