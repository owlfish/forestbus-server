/*
The commitlog package contains the raft commit log manipulation logic.  It doesn't implement any storage logic which is handled through the LogStorage interface.
*/
package commitlog

import (
	"code.google.com/p/forestbus.server/model"
	"code.google.com/p/forestbus.server/utils"
	"errors"
	"sync"
	"time"
)

// LogStorage is the interface between CommitLog and the storage mechanism.
type LogStorage interface {
	// GetMessage returns a set of approximately count messages starting at index
	// The number of messages returned may be lower or higher than the number requested
	// If only one message is requested and it exists then it will be returned
	// If index > lastIndex then GetMessages returns an empty list with no error
	GetMessages(index int64, count int64) (model.Messages, error)

	// AppendMessages writes the given messages to the end of the log.
	// CommitLog is responsible for ensuring consistency - the LogStorage needs to just append the messages
	AppendMessages(msgs model.Messages) (lastIndex int64, err error)

	// AppendFirstMessages is used by the commit log to record the first messages in the log.
	// leaderFirstIndex is used if the log is empty to determine the first real index of the log (not always 1)
	AppendFirstMessages(msgs model.Messages, leaderFirstIndex int64) (lastIndex int64, err error)

	// Commit records any messages stored with AppendMessages to the underlying storage.
	Sync() error

	// GetLastIndex returns the index of the last message in the log.
	// A lastIndex of 0 is returned if there are no messages.
	GetLastIndex() (lastIndex int64, err error)

	// GetFirstIndex returns the index of the first message in the log.
	// A lastIndex of 0 is returned if there are no messages.
	GetFirstIndex() (firstIndex int64, err error)

	// TruncateMessages is used by CommitLog to remove messages that it knows are invalid (happens during leadership changes)
	// Truncate removes messages at this index and beyond, i.e. TruncateMessages(1) removes all messages
	TruncateMessages(index int64) error

	// ExpVar returns stats for ExpVar
	ExpVar() interface{}

	// Shutdown frees any resources used by the storage and closes any maintenance goroutines.
	Shutdown(notifier *utils.ShutdownNotifier)
}

// MAX_MESSAGE_RESULTS is the maximum number of messages that a client may request.  In practise more may be returned.
const MAX_MESSAGE_RESULTS = 500

// MAX_RETRIEVE_MESSAGE_COUNT is the maximum number of messages that a leader node may send to a follower node that is behind.
const MAX_RETRIEVE_MESSAGE_COUNT = 2500

type DiskSyncPolicy int

const (
	// The WRITE_SYNC policy triggers a call to Sync the underlying storage after each completed Append.  Required for full Raft compliancy.
	WRITE_SYNC DiskSyncPolicy = iota
	// The PERIODIC_SYNC policy triggers the call to Sync periodically (set by DEFAULT_PERIODIC_SYNC_PERIOD)
	PERIODIC_SYNC
	// OSSYNC does not explicitly call Sync on the underlying storage and allows the OS to decide when to sync data to disk.  This provides the best performance.
	OSSYNC
)

/*
The commitlog can implement different disk sync policies.  The default is to allow the operating system to decide when to sync the file contents to disk.  This breaks the Raft algorithm but is required to achieve maximum performance.

Both Periodic Sync and Write Sync on linux ext4 results in stalls that trigger leadership elections.

Configuration of the policy using the admin tool isn't supported yet.
*/
const DEFAULT_SYNC_POLICY = OSSYNC

// DEFAULT_PERIODIC_SYNC_PERIOD defines how frequently Sync to storage is called when using the PERIODIC_SYNC sync policy.
const DEFAULT_PERIODIC_SYNC_PERIOD = 5 * time.Second

/*
The CommitLog is the api into the Raft commit log for the server.  One instance is created per topic.
*/
type CommitLog struct {
	// Lock is only required to be held for methods that touch commitIndex
	lock sync.RWMutex
	// commitIndex is the last seen commit index by the log.  This determine what messages are made available through the Get method used by Forest Bus clients.
	commitIndex int64
	// waitingReaders is used to all clients to block waiting on new messages to become available when the commitIndex changes.
	waitingReaders *sync.Cond

	// The log is the logStorage (A DiskLogStorage for production use) that provides message retrieval and storage.
	log LogStorage
	// The syncPolicy to apply to the underlying storage.
	syncPolicy DiskSyncPolicy
	// node_log is the log.Printf function with the topic name as a prefix.
	node_log utils.PrintFFunc
	// inShutdown indicates whether this commit log is going through it's shutdown routine.
	inShutdown bool
}

/*
CommitLogInfo is a container structure for recording information on the state of the CommitLog for use with ExpVar.
*/
type CommitLogInfo struct {
	LastCommittedID int64
	Storage         interface{}
}

/*
LoadLogInfo is used at startup by the Node to read in the persisted state.
PersistentLog's are expected at this point to validate their storage and determine the current state.

	commitIndex is the last committed index ID.
	nextIndex is the index ID for the next message to be appended to this log.
	error is populated if there are any issues loadnig the persisted state that are unrecoverable from.
*/
func (clog *CommitLog) SetupLog(topicName string, store LogStorage) (err error) {
	clog.waitingReaders = sync.NewCond(clog.lock.RLocker())
	clog.node_log = utils.GetTopicLogger(topicName, "CommitLog")
	clog.inShutdown = false
	clog.log = store
	clog.commitIndex = 0
	clog.syncPolicy = DEFAULT_SYNC_POLICY
	if clog.syncPolicy == PERIODIC_SYNC {
		go func() {
			timer := time.NewTimer(DEFAULT_PERIODIC_SYNC_PERIOD)
			for {
				<-timer.C
				clog.log.Sync()
				timer.Reset(DEFAULT_PERIODIC_SYNC_PERIOD)
			}
		}()
	}
	return nil
}

/*
Append logs the given message and returns the index of the next slot or an error if the message could not be logged.
If the end of the log doesn't match the previousIndex and the previousTerm the append must fail and previousMatch should be false.
If there is an existing entry at msg.Index with a different msg.Term then this entry and all subsequent entries must be deleted prior to the append.

NOTE: The lock is not held in this method as we do not touch commitIndex or waitingReaders
*/
func (clog *CommitLog) Append(msgs model.Messages, previousIndex int64, previousTerm int64, leaderFirstIndex int64) (nextIndex int64, previousMatch bool, err error) {
	overlapMessageCount := 0
	//clog.node_log("Recieved messages to append %v\n", msgs)

	lastIndex, err := clog.log.GetLastIndex()
	nextIndex = lastIndex + 1
	if err != nil {
		clog.node_log("Error getting last message details during append: %v\n", err)
		return 0, false, err
	}

	//clog.node_log("LastIndex is %v, previousIndex passed in is %v\n", lastIndex, previousIndex)
	if lastIndex > 0 {
		// Get our copy of the message at the peviousIndex
		previousMessageList, err := clog.log.GetMessages(previousIndex, 1)

		if err != nil {
			return nextIndex, false, err
		}
		if previousMessageList.GetCount() < 1 {
			clog.node_log("Previous index given by leader (%v) is greater than or smaller than our last index (%v), returning no match\n", previousIndex, lastIndex)

			return nextIndex, false, nil
		}

		previousMessageTerm, err := previousMessageList.GetMessageTerm(0)
		if err != nil {
			clog.node_log("Error getting previous term: %v\n", err)
			return nextIndex, false, err
		}

		if previousMessageTerm != previousTerm {
			// Term of the message at this index doesn't match.
			clog.node_log("Previous term of %v doesn't match previousMessageTerm of %v\n", previousTerm, previousMessageTerm)
			return nextIndex, false, nil
		}

		//clog.node_log("Our last index is %v.  Leader thinks our last index is %v and we are trying to append %v messages\n", lastIndex, previousIndex, msgs.GetCount())
		// How many messages overlap?  If len(msgs) is shorter than the gap between lastIndex and previousIndex, use that.
		// How many messages overlap?  If we have 10 messages and previousIndex was 8 then we have two left to check (9 & 10)
		// How many messages overlap?  If we have a lastIndex of 10 and previousIndex of 8 then we have two left to check (9 & 10)
		msgsToCheck := msgs.GetCount()
		// We have already checked the first message, does the rest push us beyond the lastIndex?
		// E.g. len (msgs) = 2, previousIndex = 5, lastIndex = 7.
		if (int64(msgsToCheck) + previousIndex) >= lastIndex {
			msgsToCheck = int(lastIndex - previousIndex)
		}

		// Next question - do all messages beyond the previousMessage match the contents of the new messages?
		checkMessagesIndex := previousIndex

		if msgsToCheck > 0 {
			overlappingMessages, err := clog.getAtLeastMessages(checkMessagesIndex+1, int64(msgsToCheck))
			clog.node_log("Retrieved %v messages from index %v to check for overlaps (needed at least %v)\n", overlappingMessages.GetCount(), checkMessagesIndex+1, msgsToCheck)
			if err != nil {
				return nextIndex, false, err
			}

			for i := 0; i < overlappingMessages.GetCount(); i++ {
				// Check terms match for the given index
				// TODO: Check payload and CRC as well?
				checkMessagesIndex++
				//clog.node_log("Loop index %v, overlapMessageCount: %v\n", i, overlapMessageCount)
				overlappingMessagesTerm, err := overlappingMessages.GetMessageTerm(i)
				if err != nil {
					return nextIndex, false, err
				}
				msgsTerm, err := msgs.GetMessageTerm(overlapMessageCount)
				if err != nil {
					return nextIndex, false, err
				}
				if overlappingMessagesTerm != msgsTerm {
					// We need to truncate from here to the end of the log.
					clog.node_log("Truncating log due to mismatching terms to index %v.\n", checkMessagesIndex)
					clog.log.TruncateMessages(int64(checkMessagesIndex))
					break
				}
				// If we get this far then we have an overlapping message
				overlapMessageCount++
			}
		}
	} else {
		if previousIndex != 0 {
			clog.node_log("Empty log, but previousIndex of %v given by leader\n", previousIndex)
			return nextIndex, false, nil
		}
		// We have our first messages - use AppendFirstMessages to set the index correctly (first message isn't always 1)
		if msgs.GetCount() > 0 {
			//clog.node_log("Attempting to append %v messages\n", len(msgsToAppend))
			lastID, err := clog.log.AppendFirstMessages(msgs, leaderFirstIndex)
			if err != nil {
				clog.node_log("Error attempting to Append messages to the log: %v\n", err)
				return nextIndex, false, nil
			} else if clog.syncPolicy == WRITE_SYNC {
				err = clog.log.Sync()
				if err != nil {
					clog.node_log("Error attempting to sync Append messages to the log: %v\n", err)
					return nextIndex, false, nil
				}
			}

			nextIndex = lastID + 1
			return nextIndex, true, nil
		}
	}

	// All messages beyond overlapMessageCount should now be appended.
	msgsToAppend, err := msgs.Slice(overlapMessageCount, msgs.GetCount())
	if err != nil {
		clog.node_log("Error slicing overlapping messages: %v\n", err)
		return nextIndex, false, nil
	}
	if msgsToAppend.GetCount() > 0 {
		//clog.node_log("Attempting to append %v messages\n", len(msgsToAppend))
		lastID, err := clog.log.AppendMessages(msgsToAppend)
		if err != nil {
			clog.node_log("Error attempting to Append messages to the log: %v\n", err)
			return nextIndex, false, nil
		} else if clog.syncPolicy == WRITE_SYNC {
			err = clog.log.Sync()
			if err != nil {
				clog.node_log("Error attempting to sync Append messages to the log: %v\n", err)
				return nextIndex, false, nil
			}
		}

		nextIndex = lastID + 1
	} else {
		//clog.node_log("Call to append, but not message to add.\n")
	}

	return nextIndex, true, nil
}

/*
Queue is used to append client messages when the node is a leader.

NOTE: The lock is not held in this method as we do not touch commitIndex or waitingReaders
*/
func (clog *CommitLog) Queue(term int64, msgs [][]byte) (IDs []int64, err error) {
	//clog.node_log("Queueing messages: %v\n", msgs)

	modelMsgs := model.MessagesFromClientData(msgs)

	if modelMsgs.GetCount() == 0 {
		clog.node_log("No messages found from client for Queue.")
		return nil, errors.New("No messages found to be Queued.")
	}

	err = modelMsgs.SetMessageTerm(term)
	if err != nil {
		clog.node_log("Error setting terms for messages: %v\n", err)
		return nil, err
	}

	// Stick them on the end of the log
	lastIndex, err := clog.log.AppendMessages(modelMsgs)
	if err != nil {
		clog.node_log("Error writing message to log in Queue: %v\n", err)
		return nil, err
	} else if clog.syncPolicy == WRITE_SYNC {
		err = clog.log.Sync()
		if err != nil {
			clog.node_log("Error syncing written message to log in Queue: %v\n", err)
			return nil, err
		}
	}

	//time.Sleep(time.Millisecond * 500)

	results := make([]int64, len(msgs))

	// If log has one message, and two are added: lastIndex (3) - len (modelMsgs (2)) + 1 = 2
	index := lastIndex - int64(modelMsgs.GetCount()) + 1
	for i := 0; i < modelMsgs.GetCount(); i++ {
		results[i] = index
		index++
	}

	return results, nil
}

/*
Get retrieves messages from the log, starting at ID
If there is no message ID and waitForMessage is true then the method will
block until at least one message is available.

NOTE: The lock is held so we can use waitingReaders
*/
func (clog *CommitLog) Get(ID int64, quantity int, waitForMessages bool) (msgs [][]byte, nextID int64, err error) {
	clog.lock.RLock()
	defer clog.lock.RUnlock()
	maxMessageResults := int64(quantity)
	if maxMessageResults > MAX_MESSAGE_RESULTS {
		maxMessageResults = MAX_MESSAGE_RESULTS
	}
	for !clog.inShutdown {
		if clog.commitIndex >= ID {
			// We have at least one message available, return them.
			messageCount := clog.commitIndex - ID + 1
			if messageCount > maxMessageResults {
				messageCount = maxMessageResults
			}

			modelMessages, err := clog.log.GetMessages(ID, messageCount)
			if err != nil {
				clog.node_log("Error getting messages: %v\n", err)
				return msgs, ID, err
			}
			msgs, err := modelMessages.Payloads()
			if err != nil {
				clog.node_log("Error getting payloads from messages: %v\n", err)
				return msgs, ID, err
			}

			if len(msgs) == 0 {
				ourFirstID, err := clog.log.GetFirstIndex()
				if err != nil {
					clog.node_log("Error getting messages during GetFirstIndex request: %v\n", err)
					return msgs, ID, err
				}
				if ourFirstID >= ID {
					nextID = ourFirstID
				} else {
					clog.node_log("Error getting messages for index %v which is higher than our first index of %v but brought back no messages\n", ID, ourFirstID)
					return nil, ID, errors.New("Error retrieveing messages even though index is higher than our first ID.")
				}
			} else {
				nextID = ID + int64(modelMessages.GetCount())
			}
			return msgs, nextID, nil
		}
		// Nothing to send - block and re-check if we've been asked to wait
		if waitForMessages {
			clog.waitingReaders.Wait()
		} else {
			if clog.commitIndex == 0 {
				// Special case for when the whole cluster has been shutdown and no messages are available.
				return nil, 0, nil
			}
			// Return empty handed.
			return msgs, ID, nil
		}
	}
	// If we get here then the node is in shutdown - return what we have
	return msgs, ID, nil
}

/*
Shutdown is used by the Node to notify any clients waiting on the appearance of new messages
that we are in shutdown and should return empty handed.

It does not trigger shutdown of the storage.
*/
func (clog *CommitLog) Shutdown() {
	clog.lock.RLock()
	defer clog.lock.RUnlock()
	clog.inShutdown = true
	clog.waitingReaders.Broadcast()
}

/*
Retrieve is used by the leader to get messages for distribution to other peers.

If there are no messages at this ID, then previousIndex and previousTerm will be set to 0 and the first available messages will be returned.

NOTE: The lock is not held in this method as we do not touch commitIndex or waitingReaders
*/
func (clog *CommitLog) Retrieve(ID int64) (previousIndex int64, previousTerm int64, msgs model.Messages, err error) {
	// Get the previous Id and Term
	retrieveID := ID
	if ID >= 2 {
		//clog.node_log("Looking up previous entries as ID >= 2\n")
		previousIndex = ID - 1

		previousMessageList, err := clog.log.GetMessages(previousIndex, 1)
		if err != nil {
			return previousIndex, previousTerm, msgs, err
		}

		if previousMessageList.GetCount() > 0 {
			previousTerm, err = previousMessageList.GetMessageTerm(0)
			if err != nil {
				return previousIndex, previousTerm, msgs, err
			}
		} else {
			// We have no messages at this ID - use the first Index entry instead
			retrieveID, err = clog.log.GetFirstIndex()
			if err != nil {
				return previousIndex, previousTerm, msgs, err
			}
		}
	} else {
		// We've been asked for the first index
		retrieveID, err = clog.log.GetFirstIndex()
		if err != nil {
			return previousIndex, previousTerm, msgs, err
		}
	}

	// Try to get as many messages as possible.
	msgs, err = clog.log.GetMessages(retrieveID, MAX_RETRIEVE_MESSAGE_COUNT)
	if err != nil {
		clog.node_log("Error reading messages from log in retrieve: %v\n", err)
		return previousIndex, previousTerm, msgs, err
	}
	//clog.node_log("Found %v messages\n", len(msgs))
	//clog.node_log("Returning messages %v\n", msgs)
	return previousIndex, previousTerm, msgs, err

}

// Used for voting - returns the last term and index in the log
func (clog *CommitLog) LastLogEntryInfo() (lastIndex int64, lastTerm int64, err error) {
	lastIndex, err = clog.log.GetLastIndex()
	if lastIndex == 0 {
		return 0, 0, nil
	}

	msg, err := clog.log.GetMessages(lastIndex, 1)
	if err != nil {
		clog.node_log("Error retrieveing message with last index %v\n:", err)
		return lastIndex, 0, err
	}
	if msg.GetCount() < 1 {
		clog.node_log("Unable to retrieve message %v for last long entry info.\n", lastIndex)
	}
	msgTerm, err := msg.GetMessageTerm(0)
	if err != nil {
		clog.node_log("Error getting term from message with last index %v\n:", err)
		return lastIndex, 0, err
	}
	return lastIndex, msgTerm, nil
}

// Get the last index in the log
func (clog *CommitLog) LastIndex() (lastIndex int64, err error) {
	lastIndex, err = clog.log.GetLastIndex()
	return lastIndex, err
}

// Get the first index in the log
func (clog *CommitLog) FirstIndex() (firstIndex int64, err error) {
	fistIndex, err := clog.log.GetFirstIndex()
	return fistIndex, err
}

func (clog *CommitLog) GetCurrentCommit() int64 {
	clog.lock.RLock()
	defer clog.lock.RUnlock()
	return clog.commitIndex
}

/*
Commit moves all messages between the current commitIndex and index into a committed state.

NOTE: The lock is used so we can access commitIndex AND waitingReaders
*/
func (clog *CommitLog) Commit(index int64) (err error) {
	// Defer the broadcast so that it happens after the lock has been released
	defer clog.waitingReaders.Broadcast()
	clog.lock.Lock()
	defer clog.lock.Unlock()
	lastIndex, err := clog.log.GetLastIndex()
	if err != nil {
		clog.node_log("Error during Commit determining last message details: %v\n", err)
		return err
	}
	// This is required to a avoid clients trying to get messages that the cluster has commited, but that
	// this node has not yet recieved.
	if lastIndex >= index {
		clog.commitIndex = index
	} else {
		clog.commitIndex = lastIndex
	}
	return nil
}

/*
getAtLeastMessages keeps calling clog.GetMessages until the count of messages has been retrieved.

Callers must be certain that index + count < lastIndex
*/
func (clog *CommitLog) getAtLeastMessages(index, count int64) (model.Messages, error) {
	var readMessageCount int64
	indexToRead := index
	var results model.Messages
	var err error
	for readMessageCount < count && err == nil {
		var msgs model.Messages
		msgs, err = clog.log.GetMessages(indexToRead, count-readMessageCount)
		if err != nil {
			return model.EMPTY_MESSAGES, err
		}
		if msgs.GetCount() > 0 {
			readMessageCount += int64(msgs.GetCount())
			indexToRead += int64(msgs.GetCount())
			results, err = results.Join(msgs)
			if err != nil {
				return model.EMPTY_MESSAGES, err
			}
		}
	}
	return results, err
}

func (clog *CommitLog) ExpVar() interface{} {
	stats := &CommitLogInfo{}
	clog.lock.RLock()
	stats.LastCommittedID = clog.commitIndex
	clog.lock.RUnlock()
	stats.Storage = clog.log.ExpVar()
	return stats
}

func (clog *CommitLog) GetLogStorage() LogStorage {
	return clog.log
}
