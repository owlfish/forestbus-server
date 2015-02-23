package memlog

import (
	"sync"
)

/*
MemoryTopicStore is used for testing the commitlog and is not for production use.
*/
type MemoryTopicStore struct {
	term     int64
	votedFor string
	lock     sync.RWMutex
}

// Load reads the given topic information from persistent storage
func (topicStore *MemoryTopicStore) Load() (term int64, votedFor string, err error) {
	return 0, "", nil
}

// SetTerm persists the term and vote
func (topicStore *MemoryTopicStore) SetTerm(term int64, votedFor string) (err error) {
	topicStore.lock.Lock()
	defer topicStore.lock.Unlock()
	topicStore.term = term
	topicStore.votedFor = votedFor
	return nil
}
