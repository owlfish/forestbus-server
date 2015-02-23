package disklog

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
)

const TOPIC_STORE_NAME = "topic-store"

type DiskTopicPersistentStore struct {
	Term     int64
	VotedFor string
	lock     sync.Mutex
	path     string
}

func NewDiskTopicPersistentStore(dirpath string) *DiskTopicPersistentStore {
	store := &DiskTopicPersistentStore{path: path.Join(dirpath, TOPIC_STORE_NAME)}
	return store
}

func (store *DiskTopicPersistentStore) SetTerm(term int64, votedFor string) (err error) {
	log.Printf("DISKLOG - Saving Store with term %v, voted for: %v\n", term, votedFor)
	store.lock.Lock()
	defer store.lock.Unlock()
	file, err := os.Create(store.path)
	defer file.Close()
	if err != nil {
		log.Printf("DISKLOG-Error creating topic store information at path %v: %v\n", store.path, err)
		return err
	}

	store.Term = term
	store.VotedFor = votedFor

	data, err := json.Marshal(store)

	if err != nil {
		log.Printf("Error creating json for path %v: %v\n", err)
		return err
	}

	_, err = file.Write(data)
	if err != nil {
		log.Printf("DISKLOG-Error writing topic store information at path %v: %v\n", store.path, err)
		return err
	}
	return nil
}

// Load reads the given topic information from persistent storage
func (store *DiskTopicPersistentStore) Load() (term int64, votedFor string, err error) {
	store.lock.Lock()
	defer store.lock.Unlock()
	file, err := os.Open(store.path)
	defer file.Close()
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("No topic store found at path %v\n", store.path)
			return 0, "", nil
		} else {
			log.Printf("DISKLOG-Error reading topic store information at path %v: %v\n", store.path, err)
			return 0, "", err
		}
	}

	// It's a short file - so just read the lot.
	data, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("DISKLOG-Error reading topic store information at path %v: %v\n", store.path, err)
		return 0, "", err
	}

	err = json.Unmarshal(data, store)
	if err != nil {
		log.Printf("Error reading json for path %v: %v\n", err)
		return 0, "", err
	}

	return store.Term, store.VotedFor, nil
}
