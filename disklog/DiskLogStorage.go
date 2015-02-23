/*
disklog is the package that implements the main production suitable implementation of the LogStorage interface.
The implementation expects a dedicated directory for the log files.  Filenames consist of the first
ID included in the file.
*/
package disklog

import (
	"code.google.com/p/forestbus.server/model"
	"code.google.com/p/forestbus.server/utils"
	"errors"
	"log"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// LOG_NAME_FORMAT is the glob matching our logfile filenames.
const LOG_NAME_FORMAT = "*-forest-log"
const LOG_NAME_FORMAT_SUFFIX = "-forest-log"

// TARGET_OPEN_SEGMETNS is the minimum number of segments to keep open
const TARGET_OPEN_SEGMENTS = 2

// SEGMENT_LAST_USED_TIMEOUT is the duration for which a segment must not have been used prior to it being closed.
const SEGMENT_LAST_USED_TIMEOUT = 2 * time.Minute

// SEGMENT_CLEANUP_SCAN_INTERVAL is the interval at which a check is run to look for segments to clean up.
const SEGMENT_CLEANUP_SCAN_INTERVAL = 1 * time.Hour

// DEFAULT_TARGET_MAX_SEGMENT_SIZE is the size in MB after which we declare the segment full
const DEFAULT_TARGET_MAX_SEGMENT_SIZE = 1024

// DEFAULT_CACHE_SLOT_SIZE is how many batches of messages are to be kept in cache.
const DEFAULT_CACHE_SLOT_SIZE = 30

var APPEND_WITH_NO_CONTENT = errors.New("No messages were given, unable to append.")
var ERR_INDEX_TOO_HIGH = errors.New("Index for message too high")
var FIRST_APPEND_MULTIPLE_SEGMENTS = errors.New("First append attempted, but multiple segments already exist.")

var ERR_SEGMENT_MESSAGE_BODY_UNREADABLE = errors.New("Error reading message body - length in header and available bytes in stream did not match.")

// ERR_SEGMENT_ALREADY_OPEN is returned if Open is called on an already open segment.
var ERR_SEGMENT_ALREADY_OPEN = errors.New("Segment is already open")

// DiskLogStorage is the main implementation of commitlog.LogStorage intended for production use.
// It maintains a persistent log to disk using a set of segment files that together hold all of the log entries.
// A cache is used to enable fast recall of messages in the log that have just been written, this ensures
// that the usual case of a leader sending messages to a follower, or a cilent reading recently written
// messages is fast and requires no physical IO.
type DiskLogStorage struct {
	topicName                     string
	pathName                      string
	segments                      []*Segment
	cache                         *MessageCache
	lock                          sync.RWMutex
	target_max_segment_size       int
	cache_slot_size               int
	segment_cleanup_age           time.Duration
	closeSegementsChannel         chan int
	segmentCleanupShutdownChannel chan *utils.ShutdownNotifier
	closeSegmentsShutdownChannel  chan *utils.ShutdownNotifier
	node_log                      utils.PrintFFunc
}

type DiskLogStorageInfo struct {
	PathName                string
	SegmentCount            int
	LastIndex               int64
	FirstIndex              int64
	Target_Max_Segment_Size int
	Segment_Cleanup_Age     string
	Cache_Slot_Size         int
	SegmentInfo             []interface{}
	CacheInfo               interface{}
}

type DiskLogConfigFunction func(*DiskLogStorage)

func SetTargetSegmentSize(size int) DiskLogConfigFunction {
	return func(dlog *DiskLogStorage) {
		dlog.target_max_segment_size = size
		if len(dlog.segments) > 0 {
			// Now apply changes to existing segments.
			lastSegment := dlog.segments[len(dlog.segments)-1]
			if lastSegment.GetOpenStatus() {
				// Last segment is open - change the configured values.
				lastSegment.target_max_segment_size = dlog.target_max_segment_size
			}
		}
	}
}

func SetCacheSlotSize(size int) DiskLogConfigFunction {
	return func(dlog *DiskLogStorage) {
		dlog.cache_slot_size = size
	}
}

func SetSegmentCleanupAge(age time.Duration) DiskLogConfigFunction {
	return func(dlog *DiskLogStorage) {
		dlog.segment_cleanup_age = age
	}
}

/*
LoadLog will open an existing DiskLogStorage structure from the given path or create a new one.

If existing files are found matching the LOG_NAME_FORMAT file format, then:

	1 - Get a list of all files match the pattern [0-9]*-forest-log
	2 - Sorts these and determine the highest index start value
	3 - Validates all message CRC in the highest index value Logfile in case there was previously a crash
	4 - Opens the highest index value Logfile and generates the index / offset slice

If no existing log files are found then a new segment is created for first Index 1.
*/
func LoadLog(topicName string, loadpath string, configFuncs ...DiskLogConfigFunction) (*DiskLogStorage, error) {
	dlog := &DiskLogStorage{pathName: loadpath, target_max_segment_size: DEFAULT_TARGET_MAX_SEGMENT_SIZE, cache_slot_size: DEFAULT_CACHE_SLOT_SIZE}
	for _, conf := range configFuncs {
		conf(dlog)
	}
	dlog.node_log = utils.GetTopicLogger(topicName, "DiskLogStorage")
	dlog.topicName = topicName

	dlog.segments = make([]*Segment, 0)
	dlog.cache = CreateCache(dlog.cache_slot_size)
	matches, err := filepath.Glob(path.Join(loadpath, LOG_NAME_FORMAT))
	dlog.node_log("Found %v segment filename matches\n", len(matches))
	if err != nil {
		dlog.node_log("Errror opening disk log: %v\n", err)
		return nil, err
	}

	if len(matches) == 0 {
		dlog.node_log("Creating new disk log in %v\n", loadpath)
		firstSegment, err := CreateNewSegment(dlog.topicName, loadpath, 1, dlog.target_max_segment_size)
		if err != nil {
			dlog.node_log("Error trying to create a new disk log storage at location %v: %v\n", loadpath, err)
			return nil, err
		}
		dlog.segments = append(dlog.segments, firstSegment)
	} else {
		dlog.node_log("Loading existing disk log from %v\n", loadpath)
		sort.Strings(matches)
		for _, filename := range matches {
			dlog.segments = append(dlog.segments, ExistingSegment(topicName, filename, dlog.target_max_segment_size))
		}
		// Open and validate the last segment as this is what we'll write to
		err = dlog.segments[len(dlog.segments)-1].Open(true)
		if err != nil {
			dlog.node_log("Error opening latest segment.")
			return nil, err
		}
	}

	// Setup a goroutine to handle the closing of open segments that are not in use
	dlog.closeSegementsChannel = make(chan int)
	dlog.closeSegmentsShutdownChannel = make(chan *utils.ShutdownNotifier, 1)
	go dlog.closeSegmentsLoop()

	// Setup a goroutine to periodically check for segments to cleanup
	dlog.segmentCleanupShutdownChannel = make(chan *utils.ShutdownNotifier, 1)
	go dlog.cleanupSegmentsLoop()
	return dlog, err
}

/*
ChangeConfiguration takes one or more DisklogConfigFunctions and applies to them to a live DiskLogStorage instance.

Changes to segment configuration will only be carried out to the latest open configuration and new segments.
*/
func (dlog *DiskLogStorage) ChangeConfiguration(configFuncs ...DiskLogConfigFunction) {
	dlog.lock.Lock()
	// First apply the changes our copy of the values.  This will impact new segment creation.
	for _, conf := range configFuncs {
		conf(dlog)
	}
	dlog.lock.Unlock()
}

/*
GetMessages returns as many messages as possible from the given start index up to count messages.

The following sequence is used to get the messages:

	1 - Check the cache.  If a message exists at this index, return it and all other messages in the cache up to count messages.
	2 - Work backwards through the segments list looking for a segment with a start Index <= index
	3 - Ask the segment to retrieve up to count messages (this may trigger a segment load)

*/
func (dlog *DiskLogStorage) GetMessages(index int64, count int64) (model.Messages, error) {
	//dlog.node_log("GetMessaages for index %v, count %v\n", index, count)
	// See if we have a cache hit
	if count < 1 {
		return model.EMPTY_MESSAGES, nil
	}
	dlog.lock.RLock()
	defer dlog.lock.RUnlock()
	if dlog.segments[len(dlog.segments)-1].GetLastIndex() < index {
		// We don't have a message at this index
		return model.EMPTY_MESSAGES, nil
	}
	cacheResults := dlog.cache.GetMessages(index, count)
	if cacheResults.GetCount() > 0 {
		return cacheResults, nil
	}

	// Work backwards through the segments looking for the one that contains this index
	for i := len(dlog.segments) - 1; i >= 0; i-- {
		seg := dlog.segments[i]
		if seg.GetFirstIndex() <= index {
			// Found it!
			msgs, segmentRequiredOpening, err := seg.GetMessages(index, int(count))
			if segmentRequiredOpening {
				dlog.node_log("Segment required opening - kicking off scan for any old segments that can be closed.\n")
				// Tigger a scan for segments to close
				select {
				case dlog.closeSegementsChannel <- 1:
					dlog.node_log("Sent message to close segment loop to scan for segments to close.\n")
				default:
					dlog.node_log("Unable to send message to close segment loop as channel is full\n")
				}
			}
			return msgs, err
		}
	}

	// We do not have a message for this index.
	return model.EMPTY_MESSAGES, nil
}

/*
AppendMessages writes the given messages into a segment and presents it to the cache.

The following steps are followed:

	1 - Attempt to append to the current last open segment.
	2 - If the segment is full, open a new segment.
	3 - If more than TARGET_OPEN_SEGMENTS (2) segments are open then close the older ones until just the previous and new segments are open.
	4 - Present the new messages to the cache
*/
func (dlog *DiskLogStorage) AppendMessages(msgs model.Messages) (lastIndex int64, err error) {
	dlog.lock.Lock()
	defer dlog.lock.Unlock()
	//dlog.node_log("AppendMessages called with message count %v\n", len(msgs))
	if msgs.GetCount() == 0 {
		return 0, APPEND_WITH_NO_CONTENT
	}
	// Attempt to append to the last segment in the list
	//dlog.node_log("Segments: %v\n", dlog)
	seg := dlog.segments[len(dlog.segments)-1]
	localAppendIndex, localLastIndex, full, err := seg.AppendMessages(msgs)
	if err != nil {
		return 0, err
	}
	if full {
		dlog.node_log("Creating a new segment with start index %v\n", localLastIndex+1)
		// Open a new segment.
		newSegment, err := CreateNewSegment(dlog.topicName, dlog.pathName, localLastIndex+1, dlog.target_max_segment_size)
		if err != nil {
			dlog.node_log("Error trying to create a new disk log storage at location %v: %v\n", dlog.pathName, err)
			return 0, err
		}
		dlog.segments = append(dlog.segments, newSegment)

		// Re-attempt the append
		dlog.node_log("Re-attempting the append after creating segment.\n")
		localAppendIndex, localLastIndex, full, err = newSegment.AppendMessages(msgs)
		if err != nil {
			dlog.node_log("Error in append: %v\n", err)
			return 0, err
		} else {
			dlog.node_log("Re-attempt at append worked.  Checking whether to close segments.\n")
		}

		// Tigger a scan for segments to close
		select {
		case dlog.closeSegementsChannel <- 1:
			dlog.node_log("Sent message to close segment loop to scan for segments to close.\n")
		default:
			dlog.node_log("Unable to send message to close segment loop as channel is full\n")
		}
	}
	// Give the messages to the cache
	dlog.cache.AppendMessages(localAppendIndex, msgs)

	return localLastIndex, nil
}

/*
AppendFirstMessages writes the given messages into a segment and presents it to the cache.

The following steps are followed:

	1 - Confirm that only one segment is open
	2 - Check whether the firstIndex of the segment matches leaderFirstIndex
	3 - If required delete the current segment and create a new one
	4 - Append the messages and populate the cache
*/
func (dlog *DiskLogStorage) AppendFirstMessages(msgs model.Messages, leaderFirstIndex int64) (lastIndex int64, err error) {
	dlog.lock.Lock()
	defer dlog.lock.Unlock()
	//dlog.node_log("AppendMessages called with message count %v\n", len(msgs))
	if msgs.GetCount() == 0 {
		return 0, APPEND_WITH_NO_CONTENT
	}

	// Check we only have one segment.
	if len(dlog.segments) != 1 {
		return 0, FIRST_APPEND_MULTIPLE_SEGMENTS
	}

	// Does our segment's first index match the leaders?
	seg := dlog.segments[0]

	// Check we have no messages.
	if seg.getMessageCount() != 0 {
		return 0, errors.New("Attempt to add first messages, but segment already has content!")
	}

	if seg.GetFirstIndex() != leaderFirstIndex {
		dlog.node_log("First index available from leader is %v, re-creating first segment.\n", leaderFirstIndex)
		err = seg.Delete()
		if err != nil {
			dlog.node_log("Error deleting existing segment: %v", err)
		}
		// Create the new one.
		seg, err = CreateNewSegment(dlog.topicName, dlog.pathName, leaderFirstIndex, dlog.target_max_segment_size)
		if err != nil {
			dlog.node_log("Error trying to create a new disk log storage at location %v: %v\n", dlog.pathName, err)
			return 0, err
		}
		// Replace the existing segment.
		dlog.segments[0] = seg
	}
	// First append is never full.
	localAppendIndex, localLastIndex, _, err := seg.AppendMessages(msgs)
	if err != nil {
		dlog.node_log("Error in first append: %v\n", err)
		return 0, err
	}
	// Give the messages to the cache
	dlog.cache.AppendMessages(localAppendIndex, msgs)

	return localLastIndex, nil
}

/*
Sync calls Sync on the underlying files that have been written to since the last sync.

This only called by the Commit Log for policies WRITE_SYNC and PERIODIC_SYNC.
*/
func (dlog *DiskLogStorage) Sync() error {
	dlog.lock.Lock()
	defer dlog.lock.Unlock()
	// Need to sync the current last segment and the one before (if it exists)
	// This assumes that segments are not opened any faster than the sync interval.
	err := dlog.segments[len(dlog.segments)-1].Sync()
	if err != nil {
		return err
	}

	if len(dlog.segments) >= 2 {
		// Do the previous segment as well.
		err := dlog.segments[len(dlog.segments)-2].Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

func (dlog *DiskLogStorage) GetLastIndex() (lastIndex int64, err error) {
	dlog.lock.RLock()
	defer dlog.lock.RUnlock()
	lastIndex = dlog.segments[len(dlog.segments)-1].GetLastIndex()
	return lastIndex, nil
}

func (dlog *DiskLogStorage) GetFirstIndex() (firstIndex int64, err error) {
	dlog.lock.RLock()
	defer dlog.lock.RUnlock()
	firstIndex = dlog.segments[0].GetFirstIndex()
	return firstIndex, nil
}

/*
TruncateMessages is used by CommitLog to remove messages that it knows are invalid (happens during leadership changes)

These steps are followed:

	1 - Truncate the messages in the cache.
	2 - The newest segment with an firstIndex <= index is found.
	3 - The segment is requested to truncate to index.
	4 - If any segments existing beyond the one found, they are deleted.
*/
func (dlog *DiskLogStorage) TruncateMessages(index int64) error {
	if index < 1 {
		return nil
	}
	dlog.lock.Lock()
	defer dlog.lock.Unlock()
	// Clear the cache first
	dlog.node_log("TruncateMessages called for index %v\n", index)
	dlog.cache.Truncate(index)

	// See which segments are impacted
	// The index of the last segment to keep
	lastSegmentToKeep := len(dlog.segments) - 1
	for i := 0; i < len(dlog.segments); i++ {
		if dlog.segments[i].GetLastIndex() >= index {
			// This segment contains messages to be removed.
			if dlog.segments[i].GetFirstIndex() >= index {
				// The whole segment is beyond where we should be.
				if i == 0 {
					// We always keep the first segment around.
					dlog.node_log("Found first segment should be deleted, truncating instead\n")
					dlog.segments[i].Truncate(index)
				} else {
					// We can remove this whole segment and everything beyond it.
					if lastSegmentToKeep >= i {
						lastSegmentToKeep = i - 1
						dlog.node_log("Found segments beyond %v (%v) can be deleted\n", lastSegmentToKeep, dlog.segments[i].filename)
					}
				}
			} else {
				// Only part of this segment is beyond the truncation point
				dlog.node_log("Found only part of segment (%v) needs to be truncated.\n", dlog.segments[i].filename)
				err := dlog.segments[i].Truncate(index)
				if err != nil {
					dlog.node_log("Error during truncate of segment (%v): %v\n", dlog.segments[i].filename, err)
					return err
				}
			}
		}
	}
	// Now remove any segments beyond lastSegmentToKeep
	for i := lastSegmentToKeep + 1; i < len(dlog.segments); i++ {
		if i > 0 {
			// We always keep the first segment.
			dlog.node_log("Truncate requires removing a segment %v\n", dlog.segments[i].filename)
			err := dlog.segments[i].Delete()
			if err != nil {
				dlog.node_log("Error removing segment file.\n")
				return err
			}
		}
	}
	if lastSegmentToKeep > 0 {
		dlog.segments = dlog.segments[:lastSegmentToKeep+1]
	}
	return nil
}

// Returns stats for expvar
func (dlog *DiskLogStorage) ExpVar() interface{} {
	dlog.lock.RLock()
	defer dlog.lock.RUnlock()
	stats := &DiskLogStorageInfo{}
	stats.PathName = dlog.pathName
	stats.SegmentCount = len(dlog.segments)
	stats.SegmentInfo = make([]interface{}, stats.SegmentCount)
	for i, seg := range dlog.segments {
		stats.SegmentInfo[i] = seg.ExpVar()
	}
	stats.CacheInfo = dlog.cache.ExpVar()
	stats.LastIndex = dlog.segments[len(dlog.segments)-1].GetLastIndex()
	stats.FirstIndex = dlog.segments[0].GetFirstIndex()
	stats.Target_Max_Segment_Size = dlog.target_max_segment_size
	stats.Cache_Slot_Size = dlog.cache_slot_size
	stats.Segment_Cleanup_Age = dlog.segment_cleanup_age.String()
	return stats
}

// closeSegmentsLoop is an internal methods used to close segments that are no longer in use.
func (dlog *DiskLogStorage) closeSegmentsLoop() {
	var notifier *utils.ShutdownNotifier
	for {
		select {
		case notifier = <-dlog.closeSegmentsShutdownChannel:
			notifier.ShutdownDone()
			return
		case <-dlog.closeSegementsChannel:
			// Take a copy of the list of segments so that we don't hold the lock for too long.
			var segmentList []*Segment
			dlog.lock.RLock()
			segmentList = append(segmentList, dlog.segments...)
			dlog.lock.RUnlock()

			openCount := 0
			for i := len(segmentList) - 1; i >= 0; i-- {
				if segmentList[i].GetOpenStatus() {
					openCount++
					if openCount > TARGET_OPEN_SEGMENTS {
						dlog.node_log("Found more than %v segments open\n", TARGET_OPEN_SEGMENTS)
						if time.Since(segmentList[i].GetLastAccessTime()) > SEGMENT_LAST_USED_TIMEOUT {
							dlog.node_log("Found segment that has not been used in the last %v, closing\n", SEGMENT_LAST_USED_TIMEOUT)
							err := segmentList[i].Close()
							if err != nil {
								// Bad things happening here
								log.Fatalf("Unable to close segment, error: %v\n", err)
							}
						} else {
							dlog.node_log("Segment not yet timed out, skipping.\n")
						}
					}
				}
			}
			dlog.node_log("Check for segments to close is complete\n")
		}
	}
}

func (dlog *DiskLogStorage) cleanupSegmentsLoop() {
	for {
		timer := time.NewTimer(SEGMENT_CLEANUP_SCAN_INTERVAL)
		var notifier *utils.ShutdownNotifier
		select {
		case notifier = <-dlog.segmentCleanupShutdownChannel:
			notifier.ShutdownDone()
			return
		case <-timer.C:
			// Check whether any clean-up has been configured.
			dlog.lock.RLock()
			cleanAge := dlog.segment_cleanup_age
			segmentsToCheck := len(dlog.segments) - 1
			dlog.lock.RUnlock()
			if cleanAge > 0 && segmentsToCheck > 0 {
				// If we have a clean-up interval we need to do a full lock while we build a list of segments to delete
				dlog.node_log("Scanning for segments that can be cleaned-up (older than %v)\n", cleanAge)
				segmentsToDelete := make([]*Segment, 0, 10)
				dlog.lock.Lock()
				// Holding the lock, find the segments that may need cleaning.  We always exclude the last active segment.
				shrinkIndex := 0
				for _, candidate := range dlog.segments[:len(dlog.segments)-1] {
					candidateAge := time.Since(candidate.GetLastModifiedTime())
					if candidateAge > cleanAge {
						dlog.node_log("Segment %v last modified on %v - will delete\n", candidate.filename, candidate.GetLastModifiedTime())
						segmentsToDelete = append(segmentsToDelete, candidate)
						shrinkIndex += 1
					} else {
						// We do clean up in sequential order - if we hit a segment that is not due for clean up we stop here.
						break
					}
				}
				// Now shrink the segment list.  This could be done sligtly more efficiently, but it's not worth the hassle
				dlog.segments = dlog.segments[shrinkIndex:]
				dlog.lock.Unlock()
				// Do the physical deletes now that we have released the lock.
				for _, todelete := range segmentsToDelete {
					err := todelete.Delete()
					if err != nil {
						dlog.node_log("ERROR: Unable to delete old segment %v.  This file should be manually removed as it is no longer part of the segment list.\n", todelete.filename, err)
					}
				}
			}
		}
		timer.Reset(SEGMENT_CLEANUP_SCAN_INTERVAL)
	}
}

func (dlog *DiskLogStorage) Shutdown(notifier *utils.ShutdownNotifier) {
	routinesNotifier := utils.NewShutdownNotifier(2)
	dlog.closeSegmentsShutdownChannel <- routinesNotifier
	dlog.segmentCleanupShutdownChannel <- routinesNotifier
	routinesNotifier.WaitForAllDone()
	// Now close all segments.
	dlog.lock.Lock()
	for _, segment := range dlog.segments {
		segment.Close()
	}
	dlog.lock.Unlock()
	notifier.ShutdownDone()
}
