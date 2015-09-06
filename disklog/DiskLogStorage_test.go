package disklog

import (
	"bytes"
	"github.com/owlfish/forestbus-server/commitlog"
	"github.com/owlfish/forestbus-server/model"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func getLogStore() commitlog.LogStorage {
	tempDir, _ := ioutil.TempDir("", "DISK-LOG-TEST")
	log, _ := LoadLog("test", tempDir, SetTargetSegmentSize(100))
	return log
}

func TestGetMessagesEmpty(t *testing.T) {
	store := getLogStore()
	res, err := store.GetMessages(-1, -1)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(-1, 0)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(-1, 1)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(0, 0)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(0, 1)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(1, 0)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(1, 1)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
}

// This is declared in MessageCache_test.go
// func getMessages(count int) model.Messages {
// 	msgs := make(model.Messages, count)
// 	for i, _ := range msgs {
// 		msg := &model.Message{}
// 		msg.SetValues(int64(i+1), []byte{byte(i + 1)})
// 		msgs[i] = msg
// 	}
// 	return msgs
// }

func TestGetOneMessage(t *testing.T) {
	store := getLogStore()
	store.AppendMessages(getMessages(1))
	res, err := store.GetMessages(-1, -1)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(-1, 0)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(-1, 1)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(0, 0)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(0, 1)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(1, 0)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(1, 1)
	if res.GetCount() != 1 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
}

func TestAppend(t *testing.T) {
	store := getLogStore()
	lastIndex, err := store.AppendMessages(model.EMPTY_MESSAGES)
	if lastIndex != 0 || err == nil {
		t.Errorf("Unexpected result or error: %v,%v", lastIndex, err)
	}

	lastIndex, err = store.AppendMessages(getMessages(1))
	if lastIndex != 1 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", lastIndex, err)
	}

	segment := store.(*DiskLogStorage).segments[0]
	if len(segment.sparseIndexOffsetList) != 1 {
		t.Fatalf("Segment didn't have one sparse offset recorded after append!")
	}

	lastIndex, err = store.AppendMessages(getMessages(2))
	if lastIndex != 3 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", lastIndex, err)
	}
}

func TestFirstAppend(t *testing.T) {
	store := getLogStore()
	lastIndex, err := store.AppendFirstMessages(getMessages(1), 100)
	if lastIndex != 100 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", lastIndex, err)
	}

	// Check we can get them
	msgs, err := store.GetMessages(100, 1)
	if err != nil {
		t.Error(err)
	}
	if msgs.GetCount() != 1 {
		t.Errorf("Unable to get first messages.")
	}

	// Check firstIndex is working
	firstIndex, err := store.GetFirstIndex()
	if err != nil {
		t.Error(err)
	}
	if firstIndex != 100 {
		t.Errorf("Expected first index of 100, got: %v\n", firstIndex)
	}

	// We should error if we try append first again.
	lastIndex, err = store.AppendFirstMessages(getMessages(1), 100)
	if err == nil {
		t.Errorf("Expected error, got nothing.")
	}
}

func TestGetLastIndex(t *testing.T) {
	store := getLogStore()
	lastIndex, err := store.GetLastIndex()
	if lastIndex != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v, %v", lastIndex, err)
	}

	appendIndex, _ := store.AppendMessages(getMessages(1))

	lastIndex, err = store.GetLastIndex()
	if lastIndex != 1 || lastIndex != appendIndex || err != nil {
		t.Errorf("Unexpected result or error: %v, %v", lastIndex, err)
	}

	appendIndex, _ = store.AppendMessages(getMessages(2))

	lastIndex, err = store.GetLastIndex()
	if lastIndex != 3 || lastIndex != appendIndex || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", lastIndex, err)
	}

}

func TestTruncateEmptyStore(t *testing.T) {
	store := getLogStore()

	err := store.TruncateMessages(-1)
	if err != nil {
		t.Error("Error not null")
	}
	err = store.TruncateMessages(0)
	if err != nil {
		t.Error("Error not null")
	}
	err = store.TruncateMessages(1)
	if err != nil {
		t.Error("Error not null")
	}
	err = store.TruncateMessages(2)
	if err != nil {
		t.Error("Error not null")
	}
}

func TestTruncateOneMessage(t *testing.T) {
	store := getLogStore()
	store.AppendMessages(getMessages(1))
	segment := store.(*DiskLogStorage).segments[0]
	if len(segment.sparseIndexOffsetList) != 1 {
		t.Fatalf("Segement didn't have one sparse offset recorded after append!")
	}

	err := store.TruncateMessages(5)
	if err != nil {
		t.Error("Error")
	}
	lastIndex, err := store.GetLastIndex()
	if lastIndex != 1 || err != nil {
		t.Errorf("Unexpected result or error: %v, %v", lastIndex, err)
	}

	err = store.TruncateMessages(2)
	if err != nil {
		t.Error("Error")
	}
	lastIndex, err = store.GetLastIndex()
	if lastIndex != 1 || err != nil {
		t.Errorf("Unexpected result or error: %v, %v", lastIndex, err)
	}

	err = store.TruncateMessages(1)
	if err != nil {
		t.Error("Error")
	}
	lastIndex, err = store.GetLastIndex()
	if lastIndex != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v, %v", lastIndex, err)
	}
}

func TestTruncateManyMessage(t *testing.T) {
	store := getLogStore()
	store.AppendMessages(getMessages(10))
	store.AppendMessages(getMessages(20))
	store.AppendMessages(getMessages(30))

	err := store.TruncateMessages(27)
	if err != nil {
		t.Error("Error")
	}
	lastIndex, err := store.GetLastIndex()
	if lastIndex != 26 || err != nil {
		t.Errorf("Unexpected result or error: %v, %v", lastIndex, err)
	}

}

func TestTruncateAcrossSegments(t *testing.T) {
	tempDir, _ := ioutil.TempDir("", "DISK-LOG-TEST")
	store, _ := LoadLog("test", tempDir, SetTargetSegmentSize(1), SetCacheSlotSize(1))
	store.AppendMessages(getMessages(103))
	store.AppendMessages(getMessages(103))
	store.AppendMessages(getMessages(103))

	err := store.TruncateMessages(199)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	lastIndex, err := store.GetLastIndex()
	if lastIndex != 198 || err != nil {
		t.Errorf("Unexpected result or error: %v, %v", lastIndex, err)
	}
	// Check that we only have two segments left.
	if len(store.segments) != 2 {
		t.Errorf("Number of segments left after truncate is not 2: %v\n", len(store.segments))
	}

}

func TestGetOnClosedSegment(t *testing.T) {
	tempDir, _ := ioutil.TempDir("", "DISK-LOG-TEST")
	store, _ := LoadLog("test", tempDir, SetTargetSegmentSize(1), SetCacheSlotSize(1))

	store.AppendMessages(getMessages(103))
	store.AppendMessages(getMessages(103))
	store.AppendMessages(getMessages(103))
	dlog := store
	dlog.segments[0].Close()
	if dlog.segments[1].GetOpenStatus() != true {
		t.Errorf("Expected segment 1 to be open.")
	}
	if dlog.segments[2].GetOpenStatus() != true {
		t.Errorf("Expected segment 2 to be open.")
	}

	res, err := store.GetMessages(1, 20)
	if res.GetCount() < 20 || err != nil {
		t.Errorf("Result of test unexpected: %v, %v\n", res, err)
	}
	Payloads, err := res.Payloads()
	if err != nil {
		t.Errorf("Payload error: %v\n", err)
	}

	if Payloads[0][0] != byte(1) {
		t.Errorf("Unexpected value in the result.")
	}

	if dlog.segments[0].GetOpenStatus() != true {
		t.Errorf("Expected segment 0 to be open.")
	}
	if dlog.segments[1].GetOpenStatus() != true {
		t.Errorf("Expected segment 1 to be open.")
	}
	if dlog.segments[2].GetOpenStatus() != true {
		t.Errorf("Expected segment 2 to be open.")
	}

}

func TestExistingDiskLogSingleSegment(t *testing.T) {
	// Create a test segment
	tempDir, _ := ioutil.TempDir("", "DISK-LOG-TEST")
	file, _ := os.Create(path.Join(tempDir, "0000000000000000001-forest-log"))
	msgs := getMessages(2)
	msgs.Write(file)
	file.Close()

	dlog, _ := LoadLog("test", tempDir, SetTargetSegmentSize(100), SetCacheSlotSize(1))

	if dlog.segments[0].segmentOpen != true {
		t.Errorf("Existing segment was not opened.\n")
	}

	diskMsgs, err := dlog.GetMessages(1, 2)
	if err != nil {
		t.Errorf("Error getting disk messages: %v\n", err)
	}
	if bytes.Compare(msgs.RawData(), diskMsgs.RawData()) != 0 {
		t.Errorf("Data from disk and memory do not match")
	}
	// for i := 0; i < msgs.GetCount(); i++ {

	// 	if msgs[i].Term != diskMsgs[i].Term {
	// 		t.Errorf("Terms from disk and memory do not match")
	// 	}
	// 	if msgs[i].CRC != diskMsgs[i].CRC {
	// 		t.Errorf("CRC from disk and memory do not match")
	// 	}
	// 	if msgs[i].Payload[0] != diskMsgs[i].Payload[0] {
	// 		t.Errorf("CRC from disk and memory do not match")
	// 	}
	// }
}

func TestExistingDiskLogTwoSegments(t *testing.T) {
	// Create a test segment
	tempDir, _ := ioutil.TempDir("", "DISK-LOG-TEST")
	file, _ := os.Create(path.Join(tempDir, "0000000000000000001-forest-log"))
	msgs := getMessages(2)
	msgs.Write(file)
	file.Close()

	file, _ = os.Create(path.Join(tempDir, "0000000000000000003-forest-log"))
	msgs2 := getMessages(2)
	msgs2.Write(file)
	file.Close()

	dlog, _ := LoadLog("test", tempDir, SetTargetSegmentSize(100), SetCacheSlotSize(1))

	if dlog.segments[0].segmentOpen != false {
		t.Errorf("Existing segment was opened when it's not the last one.\n")
	}
	if dlog.segments[1].segmentOpen != true {
		t.Errorf("Existing segment was not opened.\n")
	}

	diskMsgs, err := dlog.GetMessages(1, 2)
	if err != nil {
		t.Errorf("Error getting disk messages: %v\n", err)
	}

	if dlog.segments[0].segmentOpen != true {
		t.Errorf("First segment was not opened when expected.\n")
	}

	if bytes.Compare(msgs.RawData(), diskMsgs.RawData()) != 0 {
		t.Errorf("Data from disk and memory do not match")
	}

	// for i, _ := range msgs {
	// 	if msgs[i].Term != diskMsgs[i].Term {
	// 		t.Errorf("Terms from disk and memory do not match")
	// 	}
	// 	if msgs[i].CRC != diskMsgs[i].CRC {
	// 		t.Errorf("CRC from disk and memory do not match")
	// 	}
	// 	if msgs[i].Payload[0] != diskMsgs[i].Payload[0] {
	// 		t.Errorf("CRC from disk and memory do not match")
	// 	}
	// }

	diskMsgs, err = dlog.GetMessages(3, 2)

	if bytes.Compare(msgs2.RawData(), diskMsgs.RawData()) != 0 {
		t.Errorf("Data from disk and memory do not match")
	}

	// for i, _ := range msgs2 {
	// 	if msgs2[i].Term != diskMsgs[i].Term {
	// 		t.Errorf("Terms from disk and memory do not match")
	// 	}
	// 	if msgs2[i].CRC != diskMsgs[i].CRC {
	// 		t.Errorf("CRC from disk and memory do not match")
	// 	}
	// 	if msgs2[i].Payload[0] != diskMsgs[i].Payload[0] {
	// 		t.Errorf("CRC from disk and memory do not match")
	// 	}
	// }
}

func TestExistingDiskLogSingleSegmentCorruptedCRC(t *testing.T) {
	// Create a test segment
	tempDir, _ := ioutil.TempDir("", "DISK-LOG-TEST")
	file, _ := os.Create(path.Join(tempDir, "0000000000000000001-forest-log"))
	msgs := getMessages(10)
	// Corrupt message index 9
	msgOffsets, _ := msgs.Offsets()
	msgs.RawData()[msgOffsets[8]+model.MESSAGE_CRC_OFFSET] += 1

	msgs.Write(file)
	file.Close()

	dlog, err := LoadLog("test", tempDir, SetTargetSegmentSize(100), SetCacheSlotSize(1))

	if err != nil {
		t.Errorf("Error loading disk log: %v\n", err)
	}

	if dlog.segments[0].segmentOpen != true {
		t.Errorf("Existing segment was not opened.\n")
	}

	lastIndex, err := dlog.GetLastIndex()
	if err != nil {
		t.Errorf("Error getting last message details: %v\n", err)
	}

	if lastIndex != 8 {
		t.Errorf("Disk log not recovered as expected, last Index %v.", lastIndex)
	}

}

func TestExistingDiskLogSingleSegmentCorruptedLength(t *testing.T) {
	// Create a test segment
	tempDir, _ := ioutil.TempDir("", "DISK-LOG-TEST")
	file, _ := os.Create(path.Join(tempDir, "0000000000000000001-forest-log"))
	msgs := getMessages(10)
	// Corrupt message index 9
	msgOffsets, _ := msgs.Offsets()
	msgs.RawData()[msgOffsets[8]+model.MESSAGE_LENGTH_OFFSET] += 10

	msgs.Write(file)
	file.Close()

	dlog, err := LoadLog("test", tempDir, SetTargetSegmentSize(100), SetCacheSlotSize(1))

	if err != nil {
		t.Fatalf("Error loading disk log: %v\n", err)
	}

	if dlog.segments[0].segmentOpen != true {
		t.Errorf("Existing segment was not opened.\n")
	}

	lastIndex, err := dlog.GetLastIndex()
	if err != nil {
		t.Errorf("Error getting last message details: %v\n", err)
	}

	if lastIndex != 8 {
		t.Errorf("Disk log not recovered as expected, last Index %v.", lastIndex)
	}

}

func TestExistingDiskLogSingleSegmentCorruptedBody(t *testing.T) {
	// Create a test segment
	tempDir, _ := ioutil.TempDir("", "DISK-LOG-TEST")
	file, _ := os.Create(path.Join(tempDir, "0000000000000000001-forest-log"))
	msgs := getMessages(10)
	// Corrupt message index 9
	msgOffsets, _ := msgs.Offsets()
	msgs.RawData()[msgOffsets[8]+model.MESSAGE_OVERHEAD] += 1

	msgs.Write(file)
	file.Close()

	dlog, err := LoadLog("test", tempDir, SetTargetSegmentSize(100), SetCacheSlotSize(1))

	if err != nil {
		t.Errorf("Error loading disk log: %v\n", err)
	}

	if dlog.segments[0].segmentOpen != true {
		t.Errorf("Existing segment was not opened.\n")
	}

	lastIndex, err := dlog.GetLastIndex()
	if err != nil {
		t.Errorf("Error getting last message details: %v\n", err)
	}

	if lastIndex != 8 {
		t.Errorf("Disk log not recovered as expected, last Index %v.", lastIndex)
	}

}

/*
TestReadAlignmentWithOffsetArraySize tests that reading messages from a disklog that
does not hit the cache returns enough values to make the next request align with the
offset cache and therefore avoid seeks.
*/
func TestReadAlignmentWithOffsetArraySize(t *testing.T) {
	// Create a test segment
	tempDir, _ := ioutil.TempDir("", "DISK-LOG-TEST")
	file, _ := os.Create(path.Join(tempDir, "0000000000000000001-forest-log"))
	file.Close()

	dlog, _ := LoadLog("test", tempDir, SetCacheSlotSize(1))

	// Populate a few thousand messages
	msgs := getMessages(20000)
	msgs.SetMessageTerm(1)
	dlog.AppendFirstMessages(msgs, 1)

	// Append some messages to clear the cache
	msgs = getMessages(1)
	msgs.SetMessageTerm(1)
	dlog.AppendMessages(msgs)

	// Try requesting some messages
	seekCount := dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	retMsgs, err := dlog.GetMessages(1, 10)
	newSeekCount := dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	if newSeekCount != seekCount+1 {
		t.Errorf("Expected one seek, but seek count increased from %v to %v\n", seekCount, newSeekCount)
	}

	if err != nil {
		t.Errorf("Error returned when requesting 10 messages: %v\n", err)
	}

	if retMsgs.GetCount() != default_index_offset_density {
		t.Errorf("Returned %v messages, rather than %v\n", retMsgs.GetCount(), default_index_offset_density)
	}

	// Check that the follow-on read doesn't increase seek count
	nextID := 1 + int64(retMsgs.GetCount())
	seekCount = dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	retMsgs, err = dlog.GetMessages(nextID, 10)
	newSeekCount = dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	if newSeekCount != seekCount {
		t.Errorf("Expected no seeks, but seek count increased from %v to %v\n", seekCount, newSeekCount)
	}

	if err != nil {
		t.Errorf("Error returned when requesting 10 messages: %v\n", err)
	}

	if retMsgs.GetCount() != default_index_offset_density {
		t.Errorf("Returned %v messages, rather than %v\n", retMsgs.GetCount(), default_index_offset_density)
	}

	// Re-run the test with a non-1 index start.
	seekCount = dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	retMsgs, err = dlog.GetMessages(60, 10)
	newSeekCount = dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	if newSeekCount != seekCount+1 {
		t.Errorf("Expected one seek, but seek count increased from %v to %v\n", seekCount, newSeekCount)
	}

	if err != nil {
		t.Errorf("Error returned when requesting 10 messages: %v\n", err)
	}

	if retMsgs.GetCount() != default_index_offset_density-60+1 {
		t.Errorf("Returned %v messages, rather than %v\n", retMsgs.GetCount(), default_index_offset_density)
	}

	// Check that the follow-on read doesn't increase seek count
	nextID = 60 + int64(retMsgs.GetCount())
	seekCount = dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	retMsgs, err = dlog.GetMessages(nextID, 10)
	newSeekCount = dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	if newSeekCount != seekCount {
		t.Errorf("Expected no seeks, but seek count increased from %v to %v\n", seekCount, newSeekCount)
	}

	if err != nil {
		t.Errorf("Error returned when requesting 10 messages: %v\n", err)
	}

	if retMsgs.GetCount() != default_index_offset_density {
		t.Errorf("Returned %v messages, rather than %v\n", retMsgs.GetCount(), default_index_offset_density)
	}

	// Re-run the test with a get that spans one recorded offsets in the sparse array
	seekCount = dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	retMsgs, err = dlog.GetMessages(60, 100)
	newSeekCount = dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	if newSeekCount != seekCount+1 {
		t.Errorf("Expected one seek, but seek count increased from %v to %v\n", seekCount, newSeekCount)
	}

	if err != nil {
		t.Errorf("Error returned when requesting 100 messages: %v\n", err)
	}

	if retMsgs.GetCount() != default_index_offset_density-60+1+100 {
		t.Errorf("Returned %v messages, rather than %v\n", retMsgs.GetCount(), default_index_offset_density)
	}

	// Check that the follow-on read doesn't increase seek count
	nextID = 60 + int64(retMsgs.GetCount())
	seekCount = dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	retMsgs, err = dlog.GetMessages(nextID, 100)
	newSeekCount = dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	if newSeekCount != seekCount {
		t.Errorf("Expected no seeks, but seek count increased from %v to %v\n", seekCount, newSeekCount)
	}

	if err != nil {
		t.Errorf("Error returned when requesting 1-0 messages: %v\n", err)
	}

	if retMsgs.GetCount() != default_index_offset_density {
		t.Errorf("Returned %v messages, rather than %v\n", retMsgs.GetCount(), default_index_offset_density)
	}

	// Re-run the test with a get that spans multiple recorded offsets and starts after the first in the sparse array
	seekCount = dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	retMsgs, err = dlog.GetMessages(160, 500)
	newSeekCount = dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	if newSeekCount != seekCount+1 {
		t.Errorf("Expected one seek, but seek count increased from %v to %v\n", seekCount, newSeekCount)
	}

	if err != nil {
		t.Errorf("Error returned when requesting 100 messages: %v\n", err)
	}

	if retMsgs.GetCount() != default_index_offset_density-60+1+500 {
		t.Errorf("Returned %v messages, rather than %v\n", retMsgs.GetCount(), default_index_offset_density)
	}

	// Check that the follow-on read doesn't increase seek count
	nextID = 160 + int64(retMsgs.GetCount())
	seekCount = dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	retMsgs, err = dlog.GetMessages(nextID, 100)
	newSeekCount = dlog.ExpVar().(*DiskLogStorageInfo).SegmentInfo[0].(*SegmentInfo).StatsSeekCount
	if newSeekCount != seekCount {
		t.Errorf("Expected no seeks, but seek count increased from %v to %v\n", seekCount, newSeekCount)
	}

	if err != nil {
		t.Errorf("Error returned when requesting 1-0 messages: %v\n", err)
	}

	if retMsgs.GetCount() != default_index_offset_density {
		t.Errorf("Returned %v messages, rather than %v\n", retMsgs.GetCount(), default_index_offset_density)
	}

}

func BenchmarkAppendSingleMessage(b *testing.B) {
	msgs := getMessages(1)
	tempDir, _ := ioutil.TempDir("", "DISK-LOG-TEST")
	store, _ := LoadLog("test", tempDir, SetTargetSegmentSize(1000000))
	for n := 0; n < b.N; n++ {

		lastIndex, err := store.AppendMessages(msgs)

		if err != nil {
			b.Errorf("Unexpected result or error: %v,%v", lastIndex, err)
		}
	}
}
