package disklog

import (
	"bufio"
	"code.google.com/p/forestbus.server/model"
	"code.google.com/p/forestbus.server/utils"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

// How frequent we record the offset of the messages we have recorded.
const default_index_offset_density = 100

// Used when first reading in the segment file.
const default_file_buffer_size = 2 * 1024 * 1024

// Start size for the discard buffer when seeking
const default_discard_buffer_size = 20 * 1024

type Segment struct {
	filename                   string
	firstIndex                 int64
	lock                       sync.RWMutex
	file                       *os.File
	segmentOpen                bool
	writesPendingSync          bool
	filePosition               int64
	fileSize                   int64
	msgCount                   int
	sparseIndexOffsetList      []int64
	target_max_segment_size    int
	lastAccessTime             time.Time
	lastModifiedTime           time.Time
	discardBuffer              []byte
	statsGetMessageCalls       int
	statsAppendMessageCalls    int
	statsNoSeekGets            int
	statsNoSeekAppends         int
	statsSeekCount             int
	statsSeekMessagesReadCount int
	node_log                   utils.PrintFFunc
}

type SegmentInfo struct {
	Filename                   string
	FirstIndex                 int64
	SegmentOpen                bool
	NumberOfMessages           int
	LastAccessTime             time.Time
	LastModifiedTime           time.Time
	StatsGetMessageCalls       int
	StatsAppendMessageCalls    int
	StatsNoSeekGets            int
	StatsNoSeekAppends         int
	StatsSeekCount             int
	StatsSeekMessagesReadCount int
}

// Create generates a new Segment file in the given directory, starting at the given index.
// The Segment is returned in an open state.
func CreateNewSegment(topicName string, directory string, firstIndex int64, targetSize int) (*Segment, error) {
	seg := &Segment{target_max_segment_size: targetSize}
	seg.lock.Lock()
	defer seg.lock.Unlock()
	seg.node_log = utils.GetTopicLogger(topicName, "Segment")

	// Work out the filename.
	fileNamePrefix := fmt.Sprintf("%019d", firstIndex)
	seg.filename = path.Join(directory, fileNamePrefix+LOG_NAME_FORMAT_SUFFIX)

	seg.firstIndex = firstIndex
	seg.node_log("Creating new segment file %v for index %v\n", seg.filename, firstIndex)
	var err error
	seg.file, err = os.Create(seg.filename)
	if err != nil {
		seg.node_log("Error opening file %v: %v\n", seg.filename, err)
		return nil, err
	}
	seg.segmentOpen = true
	seg.filePosition = 0
	seg.discardBuffer = make([]byte, 0, default_discard_buffer_size)
	seg.lastAccessTime = time.Now()
	seg.lastModifiedTime = seg.lastAccessTime
	return seg, nil
}

func ExistingSegment(topicName string, fullfilename string, targetSize int) *Segment {
	_, filename := path.Split(fullfilename)
	prefixIndex := strings.Index(filename, "-")
	firstIndex, _ := strconv.ParseInt(filename[:prefixIndex], 10, 64)

	segmentFileInfo, err := os.Stat(fullfilename)

	seg := &Segment{filename: fullfilename, firstIndex: firstIndex, target_max_segment_size: targetSize}
	seg.node_log = utils.GetTopicLogger(topicName, "Segment")
	seg.node_log("Created existing segment, first index: %v for filename: %v\n", firstIndex, filename)
	seg.lastAccessTime = time.Now()
	if err != nil {
		seg.node_log("Warning: unable to stat segment file %v: %v", fullfilename, err)
		seg.lastModifiedTime = time.Now()
	} else {
		seg.lastModifiedTime = segmentFileInfo.ModTime()
	}
	return seg
}

// Open reads the Segment from disk and generates a mapping between index and offest into the file
// If validate is true then all CRCs for the messages in the log are checked and the log is trunctaed if any are invalid.
func (seg *Segment) Open(validate bool) error {
	seg.lock.Lock()
	defer seg.lock.Unlock()
	return seg.openWhileHoldingLock(validate)
}

func (seg *Segment) openWhileHoldingLock(validate bool) error {
	if seg.segmentOpen {
		seg.node_log("Unable to open segment - already open.\n")
		return ERR_SEGMENT_ALREADY_OPEN
	}
	var err error
	seg.file, err = os.OpenFile(seg.filename, os.O_RDWR, os.ModePerm)
	if err != nil {
		seg.node_log("Error opening file %v: %v\n", seg.filename, err)
		return err
	}
	fileInfo, err := seg.file.Stat()
	if err != nil {
		seg.node_log("Error getting file info %v: %v\n", seg.filename, err)
		return err
	}
	totalFileSize := fileInfo.Size()

	seg.segmentOpen = true
	reader := bufio.NewReaderSize(seg.file, default_file_buffer_size)
	seg.discardBuffer = make([]byte, 0, default_discard_buffer_size)

	var readPosition int64
	// Populate the sparseIndexOffsetList
	reading := true
	msgHeader := make([]byte, model.MESSAGE_OVERHEAD)
	// Start with a buffer of 10k - grow if required.
	msgBody := make([]byte, 0, 10000)
	var read int
	msgIndex := seg.firstIndex
	for reading && readPosition < totalFileSize {
		msgOffset := readPosition
		read, err = io.ReadFull(reader, msgHeader)
		readPosition += int64(read)
		if err != nil && err != io.EOF {
			seg.node_log("Error reading header during opening segment %v: %v\n", seg.filename, err)
			return err
		}
		if read != model.MESSAGE_OVERHEAD {
			seg.node_log("Error reading header during opening segment %v unable to read full header.  Read: %v.\n", seg.filename, read)
			reading = false
		} else {
			_, length, _, crc := model.ParseHeader(msgHeader)
			//seg.node_log("Segment %v - message length %v\n", seg.filename, length)
			// Check for length corruption
			if int64(length) > (totalFileSize - readPosition) {
				// Length is greater than remaining file size, so we know this is corrupt.
				if validate {
					truncateTarget := readPosition - int64(read)
					seg.node_log("WARNING: Found corrupted message length in segment %v.  Will truncate file to %v length\n", seg.filename, truncateTarget)
					seg.file.Truncate(int64(truncateTarget))
					reading = false
				} else {
					// Not in validation mode - just error
					seg.node_log("Length of message in segment %v invalid\n", seg.filename)
					return errors.New("Length of message in segment is invalid.")
				}
			} else {
				if length > int32(cap(msgBody)) {
					msgBody = make([]byte, length, length)
				} else {
					msgBody = msgBody[:length]
				}
				read, err = io.ReadFull(reader, msgBody)
				//seg.node_log("Segment %v - read message body, length %v\n", seg.filename, read)
				readPosition += int64(read)
				if err != nil && err != io.EOF {
					seg.node_log("Error reading body during opening segment %v: %v\n", seg.filename, err)
					return err
				}
				// If we are validating then check for length and CRC values.
				if validate {
					if read != int(length) {
						truncateTarget := readPosition - int64(read) - model.MESSAGE_OVERHEAD
						seg.node_log("Found truncated message in the segment %v.  Will truncate file to %v length\n", seg.filename, truncateTarget)
						seg.file.Truncate(int64(truncateTarget))
						reading = false
					} else {
						// Check the CRC
						readCRC := crc32.ChecksumIEEE(msgBody)
						if crc != readCRC {
							truncateTarget := readPosition - int64(read) - model.MESSAGE_OVERHEAD
							seg.node_log("Found message with invalid CRC in the segment %v.  Will truncate file to %v length\n", seg.filename, truncateTarget)
							seg.file.Truncate(int64(truncateTarget))
							reading = false
						}
					}
				} else {
					if read != int(length) {
						// Not in validation mode - treat this as a fatal error
						return ERR_SEGMENT_MESSAGE_BODY_UNREADABLE
					}
					readCRC := crc32.ChecksumIEEE(msgBody)
					if crc != readCRC {
						seg.node_log("Segment %v - CRC error reading message log.  Msg length: %v\n", seg.filename, length)
						return errors.New("CRC error check failed.")
					}
				}
				// If still reading - store offset to this message
				if reading {
					seg.appendOffsetToIndexHoldingLock(msgIndex, msgOffset)
					seg.msgCount++
					msgIndex++
				}
			}
		}
	}
	seg.filePosition = totalFileSize
	seg.fileSize = totalFileSize
	seg.lastAccessTime = time.Now()
	seg.node_log("Segment %v now open.  First Index: %v, message count %v, last index: %v\n", seg.filename, seg.firstIndex, seg.msgCount, seg.firstIndex+int64(seg.msgCount)-1)
	return nil
}

// Close turns an open sgement into a close one.
// The file is closed and memory freed.
func (seg *Segment) Close() error {
	seg.lock.Lock()
	defer seg.lock.Unlock()
	return seg.closeWhileHoldingLock()
}

func (seg *Segment) closeWhileHoldingLock() error {
	seg.filePosition = 0
	if !seg.segmentOpen {
		return nil
	}
	// Whatever happens below, clear our our memory and set state
	seg.segmentOpen = false
	seg.sparseIndexOffsetList = make([]int64, 0)

	// if seg.writesPendingSync {
	// 	err := seg.file.Sync()
	// 	if err != nil {
	// 		seg.node_log("Error closing segment %v - file sync failed: %v\n", seg.filename, err)
	// 		return err
	// 	}
	// 	seg.writesPendingSync = false
	// }

	err := seg.file.Close()
	if err != nil {
		seg.node_log("Error closing segment %v - file close failed: %v\n", seg.filename, err)
		return err
	}
	return nil
}

// Sync syncs the file if required.
func (seg *Segment) Sync() error {
	seg.lock.Lock()
	defer seg.lock.Unlock()
	if !seg.segmentOpen || !seg.writesPendingSync {
		return nil
	}

	err := seg.file.Sync()
	if err != nil {
		seg.node_log("Error committing segment %v - file sync failed: %v\n", seg.filename, err)
		return err
	}
	return nil
}

// Truncate removes all messages at index and beyond
func (seg *Segment) Truncate(index int64) error {
	seg.lock.Lock()
	defer seg.lock.Unlock()

	if !seg.segmentOpen {
		err := seg.openWhileHoldingLock(false)
		if err != nil {
			return err
		}
	}

	seg.node_log("Segment %v truncating to index %v.  First index is %v, length is %v, last index is %v\n", seg.filename, index, seg.firstIndex, seg.msgCount, int64(seg.msgCount)+seg.firstIndex-1)

	localIndex := index - seg.firstIndex

	if localIndex >= int64(seg.msgCount) {
		seg.node_log("LocalIndex %v greater than or equal to message count %v\n", localIndex, seg.msgCount)
		return nil
	}

	seg.node_log("Going to seek to index %v for truncate\n", index)
	// Jump to the offset for this index.
	offset, err := seg.seekToIndexHoldingLock(index)
	if err != nil {
		seg.node_log("Error seeking to index during truncate: %v\n", err)
		return err
	}

	seg.node_log("Seek to index %v left us at offset %v (filesize %v)\n", index, offset, seg.fileSize)
	// Truncate the file
	err = seg.file.Truncate(offset)
	if err != nil {
		seg.node_log("Error truncating segment file %v: %v\n", seg.filename, err)
		return err
	}
	seg.msgCount = int(localIndex)
	seg.truncateIndexToOffset(index)

	seg.fileSize = offset
	seg.lastAccessTime = time.Now()
	seg.lastModifiedTime = seg.lastAccessTime

	seg.node_log("Segment %v truncate result: First index is %v, length is %v, last index is %v\n", seg.filename, seg.firstIndex, seg.msgCount, int64(seg.msgCount)+seg.firstIndex-1)
	return nil
}

// Delete removes the whole segment file.
func (seg *Segment) Delete() error {
	seg.lock.Lock()
	defer seg.lock.Unlock()

	if seg.segmentOpen {
		seg.node_log("Asked to delete segment file %v.  Closing segment first.\n", seg.filename)
		err := seg.closeWhileHoldingLock()
		if err != nil {
			seg.node_log("Error during close of file %v.  Will carry on as we are attempting to delete anyway.\n", seg.filename)
		}

	}

	err := os.Remove(seg.filename)
	if err != nil {
		seg.node_log("Error trying to remove segment file %v: %v\n", seg.filename, err)
		return err
	}
	seg.sparseIndexOffsetList = nil
	seg.msgCount = 0
	seg.filePosition = 0
	seg.fileSize = 0
	return nil
}

/*
GetMessages returns as many messages as possible from the given start index up to the maximum message batch size

Cache has already been checked at this stage, so we use seeking in the file itself to find the messages.
*/
func (seg *Segment) GetMessages(index int64, targetMessageCount int) (msgs model.Messages, segmentRequiredOpening bool, err error) {
	seg.lock.Lock()
	defer seg.lock.Unlock()
	seg.statsGetMessageCalls++
	seg.lastAccessTime = time.Now()
	segmentRequiredOpening = false

	if !seg.segmentOpen {
		err := seg.openWhileHoldingLock(false)
		if err != nil {
			return model.EMPTY_MESSAGES, true, err
		}
		segmentRequiredOpening = true
	}

	localIndex := index - seg.firstIndex
	if localIndex >= int64(seg.msgCount) {
		//seg.node_log("Segment %v - requested for message index %v (local %v) which is beyond length %v\n", seg.filename, index, localIndex, len(seg.indexOffsetList))
		// Request for a message beyond our content - return empty handed.
		// This happens frequently when the leader is looking for messages that the peer hasn't seen and the peer is up to speed.
		return model.EMPTY_MESSAGES, segmentRequiredOpening, nil
	}
	_, err = seg.seekToIndexHoldingLock(index)
	if err != nil {
		return model.EMPTY_MESSAGES, false, err
	}

	// How much message data can we return?
	targetMessageBatchSize := seg.getBestReadSize(index, seg.filePosition, targetMessageCount)

	results, sizeRead, err := model.MessagesFromReader(seg.file, targetMessageBatchSize)
	seg.filePosition += int64(sizeRead)

	if err != nil {
		return model.EMPTY_MESSAGES, segmentRequiredOpening, err
	}

	if results.GetCount() == 0 {
		seg.node_log("Segment %v - requested for message index %v (local %v) which resulted in no content %v\n", seg.filename, index, localIndex, seg.msgCount)
	}

	return results, segmentRequiredOpening, nil
}

/*
AppendMessages checks to see whether the segment is full, and if not it appends the messages to it.
*/
func (seg *Segment) AppendMessages(msgs model.Messages) (appendIndex, lastIndex int64, segmentFull bool, err error) {
	seg.lock.Lock()
	defer seg.lock.Unlock()
	seg.statsAppendMessageCalls++
	seg.lastAccessTime = time.Now()
	seg.lastModifiedTime = seg.lastAccessTime

	if seg.fileSize >= int64(seg.target_max_segment_size)*1024*1024 {
		seg.node_log("Segment %v at or beyond target size of %v MB\n", seg.filename, seg.target_max_segment_size)
		return 0, seg.firstIndex + int64(seg.msgCount) - 1, true, nil
	}

	if !seg.segmentOpen {
		err := seg.openWhileHoldingLock(false)
		if err != nil {
			return 0, 0, false, err
		}
	}

	// Go to the end of the file
	if seg.filePosition != seg.fileSize {
		seg.statsSeekCount++
		seg.fileSize, err = seg.file.Seek(0, 2)
		seg.filePosition = seg.fileSize
		if err != nil {
			seg.node_log("Error seeking to end of file in segment file %v: %v\n", seg.filename, err)
			return 0, 0, false, err
		}
	} else {
		seg.statsNoSeekAppends++
	}
	appendIndex = seg.firstIndex + int64(seg.msgCount)
	//writer := bufio.NewWriter(seg.file)
	seg.writesPendingSync = true

	// Try and write out the messages
	written, err := msgs.Write(seg.file)
	if err != nil {
		seg.node_log("Error writing to segment %v: %v\n", seg.filename, err)
		return appendIndex, seg.firstIndex + int64(seg.msgCount) - 1, false, err
	}

	// Populate the offset index list
	msgs.ForEachMessage(func(msgIndex int, msgOffset int) bool {
		seg.appendOffsetToIndexHoldingLock(appendIndex+int64(msgIndex), seg.filePosition+int64(msgOffset))
		return true
	})

	seg.msgCount += msgs.GetCount()
	seg.fileSize += int64(written)
	seg.filePosition = seg.fileSize

	return appendIndex, seg.firstIndex + int64(seg.msgCount) - 1, false, nil
}

// Locking getters
func (seg *Segment) GetOpenStatus() bool {
	seg.lock.RLock()
	defer seg.lock.RUnlock()
	return seg.segmentOpen
}

func (seg *Segment) GetLastAccessTime() time.Time {
	seg.lock.RLock()
	defer seg.lock.RUnlock()
	return seg.lastAccessTime
}

func (seg *Segment) GetLastModifiedTime() time.Time {
	seg.lock.RLock()
	defer seg.lock.RUnlock()
	return seg.lastModifiedTime
}

func (seg *Segment) GetLastIndex() int64 {
	seg.lock.RLock()
	defer seg.lock.RUnlock()
	// LastIndex used with 1 message in first segment: len (1) + 1 - 1 = 1
	return int64(seg.msgCount) + seg.firstIndex - 1
}

func (seg *Segment) GetFirstIndex() int64 {
	seg.lock.RLock()
	defer seg.lock.RUnlock()
	// LastIndex used with 1 message in first segment: len (1) + 1 - 1 = 1
	return seg.firstIndex
}

func (seg *Segment) getMessageCount() int {
	seg.lock.RLock()
	defer seg.lock.RUnlock()
	return seg.msgCount
}

func (seg *Segment) truncateIndexToOffset(msgIndex int64) {
	localIndex := msgIndex - seg.firstIndex
	nearestIndex, frac := math.Modf(float64(localIndex) / float64(default_index_offset_density))
	if frac == 0 {
		// If the index that is being truncated is actually recorded, we need to go further back.
		seg.sparseIndexOffsetList = seg.sparseIndexOffsetList[:int(nearestIndex)]
	} else if len(seg.sparseIndexOffsetList) > int(nearestIndex)+1 {
		// Keep this offset around as we need it
		seg.sparseIndexOffsetList = seg.sparseIndexOffsetList[:int(nearestIndex)+1]
	}
}

func (seg *Segment) appendOffsetToIndexHoldingLock(msgIndex int64, msgOffset int64) {
	localIndex := msgIndex - seg.firstIndex
	_, frac := math.Modf(float64(localIndex) / float64(default_index_offset_density))
	// We only store offsets for n messages
	if frac != 0 {
		return
	}
	seg.sparseIndexOffsetList = append(seg.sparseIndexOffsetList, msgOffset)
}

/*
getBestReadSize returns the best guess as to how much data should be read to reach the target message count.

Logic used:

1 - If the target count is 1 message, recommend double the average message size of the segment, or 1k if empty.
2 - Find the largest number that will align the read to the sparse array of offsets.
*/
func (seg *Segment) getBestReadSize(msgIndex int64, msgPosition int64, targetCount int) (bestSize int) {
	if targetCount == 1 {
		if seg.msgCount == 0 {
			return 1024
		}
		return int(2 * seg.fileSize / int64(seg.msgCount))
	}

	localIndex := msgIndex - seg.firstIndex
	nearestIndexf, _ := math.Modf(float64(localIndex) / float64(default_index_offset_density))
	nearestIndex := int(nearestIndexf)

	// If nearest sparse index is already at the end of the file, just return end of file information.
	if nearestIndex == len(seg.sparseIndexOffsetList)-1 {
		return int(seg.fileSize - msgPosition)
	}

	// Is there an offset between the end and where we are that makes more sense?
	for i, offset := range seg.sparseIndexOffsetList[nearestIndex:] {
		if (i+nearestIndex)*default_index_offset_density-int(localIndex) >= targetCount {
			// Use this offset.
			return int(offset - msgPosition)
		}
	}

	// If our maximum is so big that we hit the end - just send the whole file.
	return int(seg.fileSize - msgPosition)

}

func (seg *Segment) seekToIndexHoldingLock(msgIndex int64) (int64, error) {
	localIndex := msgIndex - seg.firstIndex
	nearestIndexf, _ := math.Modf(float64(localIndex) / float64(default_index_offset_density))
	nearestIndex := int(nearestIndexf)
	//seg.node_log("seekToIndex: %v - local index %v - nearestIndex %v - frac %v\n", msgIndex, localIndex, nearestIndex, frac)
	nearestMsgIndex := int64(nearestIndex * default_index_offset_density)
	remainder := int(localIndex - nearestMsgIndex)
	nearestOfset := seg.sparseIndexOffsetList[nearestIndex]
	// Jump to the nearest ofset if we are not already there.
	//seg.node_log("Current file offset %v, nearest offset we need is %v\n", seg.filePosition, nearestOfset)
	if seg.filePosition != nearestOfset {
		seg.statsSeekCount++
		//seg.node_log("Had to seek was at %v, target is %v\n", seg.filePosition, nearestOfset)
		seg.file.Seek(nearestOfset, 0)
		seg.filePosition = nearestOfset
	} else if remainder == 0 {
		seg.statsNoSeekGets++
	}

	cumulativeOffset := nearestOfset

	if remainder != 0 {
		//seg.node_log("Seeking for %v messages to get to location.\n", remainder)
		// Need to read messages to move from nearest offset known to where we want to be.
		for i := 0; i < remainder; i++ {
			cumulativeOffset += model.MESSAGE_OVERHEAD
			length, err := model.ReadHeaderGetLength(seg.file)
			if err != nil {
				return 0, err
			}
			if cap(seg.discardBuffer) < length {
				seg.discardBuffer = make([]byte, 0, length*2)
			}
			n, err := io.ReadFull(seg.file, seg.discardBuffer[:length])
			if err != nil {
				return 0, err
			}
			cumulativeOffset += int64(n)
		}
		//seg.node_log("Had to seek %v messages\n", remainder)
		seg.statsSeekMessagesReadCount += remainder
		// Now in position
		seg.filePosition = cumulativeOffset
	}
	return cumulativeOffset, nil
}

func (seg *Segment) ExpVar() interface{} {
	seg.lock.RLock()
	defer seg.lock.RUnlock()
	stats := &SegmentInfo{}
	stats.Filename = seg.filename
	stats.FirstIndex = seg.firstIndex
	stats.NumberOfMessages = seg.msgCount
	stats.SegmentOpen = seg.segmentOpen
	stats.LastAccessTime = seg.lastAccessTime
	stats.LastModifiedTime = seg.lastModifiedTime
	stats.StatsGetMessageCalls = seg.statsGetMessageCalls
	stats.StatsAppendMessageCalls = seg.statsAppendMessageCalls
	stats.StatsNoSeekGets = seg.statsNoSeekGets
	stats.StatsNoSeekAppends = seg.statsNoSeekAppends
	stats.StatsSeekCount = seg.statsSeekCount
	stats.StatsSeekMessagesReadCount = seg.statsSeekMessagesReadCount
	return stats
}
