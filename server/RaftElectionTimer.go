package server

import (
	"code.google.com/p/forestbus.server/utils"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Times in milliseonds
const MIN_ELECTION_TIMEOUT = 500
const MAX_ELECTION_TIMEOUT = 700

// Time to delay the start of the first timeout period.  This avoids trying to start an
// election right at the start of our lifecycle.
const STARTUP_DELAY_TIMEOUT = NODE_CONNECTION_INTERVAL + time.Second

/*
The RaftElectionTimer manages the timeout for triggering an election.
*/
type RaftElectionTimer struct {
	running         bool
	randomGenerator *rand.Rand
	lastMessage     time.Time
	node            *Node
	lock            sync.RWMutex
	shutdownChannel chan *utils.ShutdownNotifier
}

func NewElectionTimer(node *Node) *RaftElectionTimer {
	timer := &RaftElectionTimer{shutdownChannel: make(chan *utils.ShutdownNotifier, 1)}
	timer.randomGenerator = rand.New(rand.NewSource(time.Now().UnixNano()))
	timer.running = false
	timer.node = node
	return timer
}

/*
Start triggers the running of the timeout loop.
*/
func (t *RaftElectionTimer) Start() {
	t.lock.Lock()
	defer t.lock.Unlock()
	//log.Printf("Election timer starting\n")
	t.running = true
	t.lastMessage = time.Now()
}

/*
Pause temproarily stops the timeout.  This is useful when the node is carrying out potentially long running activites (writing to disk, syncing) that will interrupt the receiving of messages from the leader.
*/
func (t *RaftElectionTimer) Pause() {
	t.lock.Lock()
	defer t.lock.Unlock()
	//log.Printf("Election timer going into pause.\n")
	t.running = false
}

func (t *RaftElectionTimer) Shutdown(notifier *utils.ShutdownNotifier) {
	t.shutdownChannel <- notifier

}

/*
The Activity method is used by the Node to record that a leader has sent through a message and that the timeout should be restarted.
*/
func (t *RaftElectionTimer) Activity() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.lastMessage = time.Now()
}

/*
RunElectionTimer runs the timer logic.  This method will also call elections and run the leadership loop.
*/
func (t *RaftElectionTimer) RunElectionTimer() {
	loopRunning := true
	timeoutMS := t.randomGenerator.Int31n(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT) + MIN_ELECTION_TIMEOUT
	timeout := time.Duration(timeoutMS) * time.Millisecond
	log.Printf("First election timeout: %v\n", timeout)

	var notifier *utils.ShutdownNotifier

	timer := time.NewTimer(STARTUP_DELAY_TIMEOUT)
	// Delay the running of the timer at startup
	<-timer.C

	timer.Reset(timeout)
	for loopRunning {
		// Block until the timer has passed
		select {
		case notifier = <-t.shutdownChannel:
			loopRunning = false
		case <-timer.C:
			t.lock.RLock()
			lastMessageDuration := time.Since(t.lastMessage)
			//log.Printf("Running %v, lastMessageDuration %v\n", t.running, lastMessageDuration)
			if t.running && lastMessageDuration > timeout {
				t.lock.RUnlock()
				// We may need to start an election
				t.node.holdElection()
			} else {
				t.lock.RUnlock()
			}

			// Set the new timer
			// TODO: Should we subtract lastMessageDuration from the new timeout?
			timeoutMS = t.randomGenerator.Int31n(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT) + MIN_ELECTION_TIMEOUT
			timeout = time.Duration(timeoutMS) * time.Millisecond
			//log.Printf("Setting timer to %v for next election\n", timeout)
			timer.Reset(timeout)
		}
	}
	notifier.ShutdownDone()
}
