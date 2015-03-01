/*
memlog is a memory based implementation of the LogStorage interface.  It is used for testing the commitlog logic seperately from the underlying disk storage.
*/
package memlog

import (
	"bytes"
	"code.google.com/p/forestbus.server/model"
	"code.google.com/p/forestbus.server/utils"
	"errors"
)

/*
MemoryLogStorage is only suitable for use in testing - it is not safe for parallel operations.  Messages are stored in memory using a byte slice and an index offset list.

*/
type MemoryLogStorage struct {
	data       []byte
	firstIndex int64
	offsets    []int
}

func (mlog *MemoryLogStorage) Shutdown(notifier *utils.ShutdownNotifier) {
	notifier.ShutdownDone()
}

func (mlog *MemoryLogStorage) GetMessages(index int64, count int64) (model.Messages, error) {
	// Index starts at leaderFirstIndex, our local index does not
	minFetch := index - mlog.firstIndex
	if minFetch < 0 || count < 1 {
		return model.EMPTY_MESSAGES, nil
	}
	if minFetch >= int64(len(mlog.offsets)) {
		return model.EMPTY_MESSAGES, nil
	}

	fetchPoint := mlog.offsets[minFetch]

	maxFetch := minFetch + int64(count)
	var targetBytes int
	if maxFetch >= int64(len(mlog.offsets)) {
		maxFetch = int64(len(mlog.offsets))
		targetBytes = len(mlog.data) - fetchPoint
	} else {
		targetBytes = mlog.offsets[maxFetch] - fetchPoint
	}

	reader := bytes.NewReader(mlog.data[fetchPoint:])

	msgs, _, err := model.MessagesFromReader(reader, targetBytes)

	return msgs, err
}

func (mlog *MemoryLogStorage) AppendMessages(msgs model.Messages) (lastIndex int64, err error) {
	if mlog.firstIndex == 0 {
		mlog.firstIndex = 1
	}
	firstOffset := len(mlog.data)
	msgs.Write(mlog)
	// Populate the offsets
	msgsOffsets, err := msgs.Offsets()
	if err != nil {
		return 0, err
	}

	for _, msgOffset := range msgsOffsets {
		mlog.offsets = append(mlog.offsets, firstOffset+msgOffset)
	}

	return int64(len(mlog.offsets)) + mlog.firstIndex - 1, nil
}

func (mlog *MemoryLogStorage) AppendFirstMessages(msgs model.Messages, leaderFirstIndex int64) (lastIndex int64, err error) {
	if len(mlog.offsets) != 0 {
		return 0, errors.New("Attempt to add first messages, but segment already has content!")
	}
	mlog.firstIndex = leaderFirstIndex
	msgs.Write(mlog)
	// Populate the offsets
	// Populate the offsets
	msgsOffsets, err := msgs.Offsets()
	if err != nil {
		return 0, err
	}

	for _, msgOffset := range msgsOffsets {
		mlog.offsets = append(mlog.offsets, msgOffset)
	}
	return int64(len(mlog.offsets)) + mlog.firstIndex - 1, nil
}

func (mlog *MemoryLogStorage) Sync() error {
	// No-op for memory based log.
	return nil
}

// TruncateMessages is used by CommitLog to remove messages that it knows are invalid (happens during leadership changes)
func (mlog *MemoryLogStorage) TruncateMessages(index int64) error {
	localIndex := index - 1
	if localIndex < 0 {
		return nil
	}
	if localIndex >= int64(len(mlog.offsets)) {
		return nil
	}
	shrinkTo := mlog.offsets[localIndex]
	mlog.data = mlog.data[:shrinkTo]
	mlog.offsets = mlog.offsets[:localIndex]
	return nil
}

func (mlog *MemoryLogStorage) GetLastIndex() (lastIndex int64, err error) {
	if len(mlog.offsets) == 0 {
		return 0, nil
	}
	return int64(len(mlog.offsets)) + mlog.firstIndex - 1, nil
}

func (mlog *MemoryLogStorage) GetFirstIndex() (firstIndex int64, err error) {
	return mlog.firstIndex, nil
}

func (mlog *MemoryLogStorage) ExpVar() interface{} {
	return nil
}

func (mlog *MemoryLogStorage) Write(newdata []byte) (int, error) {
	mlog.data = append(mlog.data, newdata...)
	return len(newdata), nil
}

func NewMemoryLogStorage() *MemoryLogStorage {
	store := &MemoryLogStorage{}
	store.data = make([]byte, 0, 100*1024)
	store.offsets = make([]int, 0, 1024)
	return store
}
