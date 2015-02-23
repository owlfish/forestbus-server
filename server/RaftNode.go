package server

import (
	"code.google.com/p/forestbus.server/commitlog"
	"code.google.com/p/forestbus.server/utils"
	"math"
	"sort"
	"sync"
	"time"
)

// NodeState tracks what state this node (topic) in the cluster is in.
type NodeState int

func (state NodeState) String() string {
	switch state {
	case STARTING_UP_NODE:
		return "Starting Up"
	case FOLLOWER_NODE:
		return "Follower"
	case CANDIDATE_NODE:
		return "Candidate"
	case LEADER_NODE:
		return "Leader"
	case ORPHAN_NODE:
		return "Orphan"
	case SHUTDOWN_NODE:
		return "Shutdown"
	}
	return "Unknown"
}

const RAFT_NODE_SUBSYSTEM_SHUTDOWN_TIMEOUT = 500 * time.Millisecond

const (
	// STARTING_UP_NODE is the initial state of a Node (topic) that is loading files, checking logs, etc at start.
	STARTING_UP_NODE NodeState = iota

	// FOLLOWER_NODE is the first working state of a Node.  Nodes in this state can serve GET requests, vote and replicate the log.
	FOLLOWER_NODE

	// CANDIDATE_NODE is entered if the Node does not believe there is a valid leader and is asking for votes.
	CANDIDATE_NODE

	// LEADER_NODE is entered if the Node recieves a majority of votes for a given term.
	// Leaders accept PUT requests and issue AppendEntries requests to other nodes
	LEADER_NODE

	// ORPHAN_NODE is a node state when this node is not part of the cluster.
	// In this state elections are not requested
	ORPHAN_NODE

	// SHUTDOWN_NODE is a node that is in the process of shutting down
	SHUTDOWN_NODE
)

/*
CommitIndex is used by the Leader Node (topic leader) to calculate what the current commit index is.
*/
type CommitIndex struct {
	firstIndexOfTerm   int64
	currentCommitIndex int64
	lock               sync.RWMutex
	waitingClients     *sync.Cond
}

func NewCommitIndex() *CommitIndex {
	c := &CommitIndex{}
	c.waitingClients = sync.NewCond(c.lock.RLocker())
	return c
}

/*
FirstIndexOfTerm is used by a Leader Node after it has won an election to inform the CommitIndex of the current index and the last commit index seen.  This is used to implement the Raft commit logic of only using majority votes to commit new messages in a leaders term.
*/
func (commInd *CommitIndex) FirstIndexOfTerm(firstInd int64, lastSeenCommit int64) {
	commInd.lock.Lock()
	defer commInd.lock.Unlock()
	commInd.firstIndexOfTerm = firstInd
	if commInd.currentCommitIndex < lastSeenCommit {
		// Use the best commit index we have.
		commInd.currentCommitIndex = lastSeenCommit
	}
}

/*
UpdateCommitIndex is used to recalculate the commit index for a topic when replication of messages to a node has been completed.

The first index of the term is used to determine whether the replication is sufficient to trigger the majority rule for commit index.
*/
func (commInd *CommitIndex) UpdateCommitIndex(node *Node) {
	commInd.lock.Lock()
	defer commInd.lock.Unlock()
	node.lock.RLock()
	defer node.lock.RUnlock()
	indexes := make([]float64, len(node.peers)+1)
	i := 0
	for _, peer := range node.peers {
		indexes[i] = float64(peer.GetLastIndex())
		i++
	}
	// Add our own index into the mix.
	ourIndex, err := node.log.LastIndex()
	ourIndexFloat := float64(ourIndex)
	indexes[len(indexes)-1] = ourIndexFloat
	if err != nil {
		node.server.RequestShutdown("Error reading commit index from log file.")
	}
	//node.node_log("Pre-sorted indexes: %v\n", indexes)
	// Sort lowest to highest
	sort.Float64s(indexes)
	//node.node_log("Post-sorted indexes: %v\n", indexes)
	// What's our cut-off point?
	// E.g. 1/2 = 0.5, index is 0.  2/2 = 1, index is 1 (i.e. both nodes).  3/2 = 1.5, index is 1 (two out of three nodes)
	cutOffIndex := int(math.Floor((float64(len(indexes)) / 2)))
	index := int64(indexes[cutOffIndex])
	if index >= commInd.firstIndexOfTerm {
		commInd.currentCommitIndex = index
		commInd.waitingClients.Broadcast()
	} else if commInd.currentCommitIndex == 0 {
		// Check for a super majority.  If all peers agree on a validated index then treat it as committed
		superMajority := true
		for _, peer := range node.peers {
			if peer.GetLastValidatedIndex() != ourIndex {
				superMajority = false
				break
			}
		}
		if superMajority {
			node.node_log("All of the peers agree on index, updating commit index.")
			commInd.currentCommitIndex = index
			commInd.waitingClients.Broadcast()
		}
	}
}

func (commInd *CommitIndex) GetCommitIndex() int64 {
	commInd.lock.RLock()
	defer commInd.lock.RUnlock()
	return commInd.currentCommitIndex
}

/*
WaitOnCommitChange returns the new commitIndex when it changes.
If the commitIndex is already further on than the index given it returns immediately.
The method does not wait until index has been reached - the caller must check whether WaitOnCommitChange is required again.
*/
func (commInd *CommitIndex) WaitOnCommitChange(index int64) int64 {
	commInd.lock.RLock()
	defer commInd.lock.RUnlock()
	// Check if we've already had a commit covering this index.
	if index <= commInd.currentCommitIndex {
		return commInd.currentCommitIndex
	}
	// If not, wait until the commit changes.
	commInd.waitingClients.Wait()
	return commInd.currentCommitIndex
}

/*
ReleaseWaitingClients is used by the Node to notify any clients waiting on the commit index
change that we are no longer leaders.
*/
func (commInd *CommitIndex) ReleaseWaitingClients() {
	commInd.lock.RLock()
	defer commInd.lock.RUnlock()
	commInd.waitingClients.Broadcast()
}

// MAX_INCOMING_MESSAGE_SIZE defines the maximum amount of data that can be read in a single go
const MAX_INCOMING_MESSAGE_SIZE = 5 * 1024 * 1024

// Ack idle ping time to avoid leadership timeouts
const LEADER_ACK_TIMEOUT = 200 * time.Millisecond

// Candidate timeout for how long to give peers to vote
const CANDIDATE_VOTE_TIMEOUT = 200 * time.Millisecond

/*
The TopicPersistentStore is an interface used by the node to store persistently the term and who the node last voted for.
*/
type TopicPersistentStore interface {
	// SetTerm persists the term and vote
	SetTerm(term int64, votedFor string) (err error)

	// Load reads the given topic information from persistent storage
	Load() (term int64, votedFor string, err error)
}

/*
Command is an internal structure used to send votes and append messages requests to the goroutines that manage interactions with the peers.
*/
type Command struct {
	Action        int
	ResultChannel chan interface{}
}

const (
	// Command to send a vote request to a peer
	ACTION_SEND_VOTE int = iota

	// Command to send the log to a peer
	ACTION_SEND_LOG

	// Send an ACK to ensure everyone knows we are still the leader
	ACTION_SEND_ACK

	// Command to shutdown the peer loop
	ACTION_SHUTDOWN
)

/*
Peer is the Node (topic)'s representation of the peers, including the leaders understanding of what the next and last index is on those peers.
*/
type Peer struct {
	Name        string
	sendMessage chan Command
	lock        sync.RWMutex
	nextIndex   int64
	// lastIndex is the RAFT matchIndex
	lastIndex int64
	// The last confirmed index with a validated previousTerm and previousIndex
	validatedIndex int64
}

// PeerInfo is the ExpVar information output for a Node's Peer.
type PeerInfo struct {
	Name      string
	NextIndex int64
	LastIndex int64
}

// SendMessage attempts to send a message to this peer.
// A RequestVote is sent if this node is in Candidate state
// An AppendEntries is sent if this node is in Leader state.
// True is returned if message delivery can be attempted, false otherwise.
func (peer *Peer) SendMessage(cmnd Command) bool {
	select {
	case peer.sendMessage <- cmnd:
		return true
	default:
		return false
	}

}

func (peer *Peer) GetLastIndex() int64 {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.lastIndex
}

func (peer *Peer) GetLastValidatedIndex() int64 {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.validatedIndex
}

func (peer *Peer) GetNextIndex() int64 {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.nextIndex
}

func (peer *Peer) SetValidatedIndex(validatedIndex, nextIndex int64) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.lastIndex = validatedIndex
	peer.validatedIndex = validatedIndex
	peer.nextIndex = nextIndex
}

func (peer *Peer) SetLastIndex(newIndex int64) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.lastIndex = newIndex
}

func (peer *Peer) SetNextIndex(newIndex int64) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.nextIndex = newIndex
}

// shutdown is used to request the shutdown of the goroutine that manages sending message to this peer.
func (peer *Peer) shutdown(notifier *utils.ShutdownNotifier) {
	go func() {
		// Send the shutdown - block if required.
		confirmedShutdown := make(chan interface{}, 0)
		cmd := Command{Action: ACTION_SHUTDOWN, ResultChannel: confirmedShutdown}
		peer.sendMessage <- cmd
		<-confirmedShutdown
		if notifier != nil {
			notifier.ShutdownDone()
		}
	}()
}

func (peer *Peer) ExpVar() interface{} {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	stats := &PeerInfo{}
	stats.Name = peer.Name
	stats.LastIndex = peer.lastIndex
	stats.NextIndex = peer.nextIndex
	return stats
}

// Node holds the state of this node (topic) in the cluster.
type Node struct {
	server          *ServerNode
	name            string
	topic           string
	state           NodeState
	currentTerm     int64
	votedFor        string
	log             *commitlog.CommitLog
	writeAggregator *QueueWriteAggregator
	store           TopicPersistentStore
	leaderNode      string
	peers           map[string]*Peer
	peerNameList    []string
	electionTimer   *RaftElectionTimer
	lock            sync.RWMutex
	shutdownServer  chan string
	commitIndex     *CommitIndex
	node_log        utils.PrintFFunc
}

// The ExpVar information structure for the Node.
type NodeInfo struct {
	Topic                string
	State                string
	CurrentTerm          int64
	VotedFor             string
	LeaderNode           string
	CommitIndex          int64
	CommitLog            interface{}
	QueueWriteAggregator interface{}
	TopicPeers           []interface{}
}

/*
StartNode starts up the Raft node implementation for a given topic.  The connections are managed by the ServerNode.  The Node provides methods that are used by the RPCHandler for interaction with the commit log.
*/
func (node *Node) StartNode(topic string, server *ServerNode, ourName string, ourPeers ConfigPeers, ourLog *commitlog.CommitLog, topicStore TopicPersistentStore) error {
	// Create the shutdown channel
	node.shutdownServer = make(chan string)
	node.node_log = utils.GetTopicLogger(topic, "Raft")
	// Apply our configuration
	node.topic = topic
	node.server = server
	node.log = ourLog
	node.store = topicStore
	node.node_log("Applying configuration\n")
	node.name = ourName
	node.peers = make(map[string]*Peer)

	node.ChangePeerConfiguration(ourPeers)

	node.commitIndex = NewCommitIndex()
	node.writeAggregator = NewQueueWriteAggregator(node)

	node.electionTimer = NewElectionTimer(node)

	// Load the log from disk
	node.node_log("Loading log info.\n")
	var err error

	node.currentTerm, node.votedFor, err = node.store.Load()
	if err != nil {
		node.node_log("Error loading persistent store: %v\n", err)
		return err
	}
	node.node_log("Log store - creating RPC handlers.\n")

	node.node_log("Election timer starting.\n")
	// Start the election timer running
	node.electionTimer.Start()
	go node.electionTimer.RunElectionTimer()

	node.node_log("Node initialisation complete.\n")

	// If we are standalone then recalculate the commit index.
	if len(node.peers) == 0 && node.state != ORPHAN_NODE {
		node.node_log("Standalone peer configuration ")
		node.commitIndex.UpdateCommitIndex(node)
		node.log.Commit(node.commitIndex.GetCommitIndex())

	}
	return nil
}

// ExpVar provides node stats when requested.
func (node *Node) ExpVar() interface{} {
	node.lock.RLock()
	defer node.lock.RUnlock()
	stats := &NodeInfo{}
	stats.Topic = node.topic
	stats.State = node.state.String()
	stats.CurrentTerm = node.currentTerm
	stats.VotedFor = node.votedFor
	stats.LeaderNode = node.leaderNode
	stats.CommitIndex = node.commitIndex.GetCommitIndex()
	stats.CommitLog = node.log.ExpVar()
	stats.QueueWriteAggregator = node.writeAggregator.ExpVar()
	stats.TopicPeers = make([]interface{}, 0, len(node.peerNameList))
	for _, peerName := range node.peerNameList {
		stats.TopicPeers = append(stats.TopicPeers, node.peers[peerName].ExpVar())
	}
	return stats
}

/*
Shutdown is used by the ServerNode to shutdown this raft node.
*/
func (node *Node) Shutdown(notifier *utils.ShutdownNotifier) {
	go func() {
		// End the leadership loop if we are in one.
		node.setState(SHUTDOWN_NODE)

		// Shutdown the election timer goroutine.
		electionShutdown := utils.NewShutdownNotifier(1)
		node.electionTimer.Shutdown(electionShutdown)

		// Have to wait on the election timer to make sure leadership loop has quit
		// Wait for the election timer to confirm closing.
		if electionShutdown.WaitForDone(RAFT_NODE_SUBSYSTEM_SHUTDOWN_TIMEOUT) != 1 {
			node.node_log("Election timer did not shutdown - proceeding anyway.\n")
		} else {
			node.node_log("Election timer shutdown completed.\n")
		}

		// Start shutdown on the commit log - this releases clients in GET.
		node.log.Shutdown()

		// Get the list of peers - need the lock for this.
		node.lock.RLock()
		peersToClose := len(node.peers)
		peerList := make([]*Peer, 0, peersToClose)
		peerShutdown := utils.NewShutdownNotifier(peersToClose)
		for _, peer := range node.peers {
			peerList = append(peerList, peer)
		}
		node.lock.RUnlock()

		// Ask for shutdown without the lock to avoid possible contention blocking the channel used to send commands to the peer goroutine.
		for _, peer := range peerList {
			peer.shutdown(peerShutdown)
		}

		// Wait for the peers to confirm closing - no timeout.
		peerShutdown.WaitForAllDone()
		node.node_log("Peers all shutdown.\n")

		// Shutdown the write aggregator
		writeAggNotifier := utils.NewShutdownNotifier(1)
		node.writeAggregator.Shutdown(writeAggNotifier)

		// Wait for the write aggregator to shutdown
		writeAggNotifier.WaitForAllDone()
		node.node_log("Write aggregator shutdown complete.")

		// Ask our storage to shutdown
		storageNotifier := utils.NewShutdownNotifier(1)
		store := node.log.GetLogStorage()
		store.Shutdown(storageNotifier)

		storageNotifier.WaitForAllDone()
		node.node_log("Storage shutdown completed.")
		notifier.ShutdownDone()
	}()
}

/*
ChangePeerConfiguration is used by both StartNode and the ServerNode (when handling configuration changes) to manage the starting and stopping of peer goroutines as the number and identity of peers changes.
*/
func (node *Node) ChangePeerConfiguration(newPeers ConfigPeers) {
	node.lock.Lock()
	defer node.lock.Unlock()

	// Remove peers that are no longer used
	for peerName, peer := range node.peers {
		if !newPeers.Contains(peerName) {
			node.node_log("Removing peer %v from configuration\n", node.topic, peerName)
			peer.shutdown(nil)
			delete(node.peers, peerName)
		}
	}

	// Add new peers - excluding ourselves.
	for _, peerName := range newPeers {
		if _, ok := node.peers[peerName]; !ok {
			if peerName != node.name {
				node.node_log("Adding peer %v to configuration\n", node.topic, peerName)
				newPeer := &Peer{}
				newPeer.Name = peerName
				newPeer.sendMessage = make(chan Command)
				node.peers[peerName] = newPeer
				go node.sendPeerMessagesLoop(newPeer)
			}
		}
	}

	// Create an updated list of peers
	node.peerNameList = make([]string, 0, len(node.peers))
	for key := range node.peers {
		node.peerNameList = append(node.peerNameList, key)
	}
	sort.Strings(node.peerNameList)

	if len(newPeers) == 0 {
		node.state = ORPHAN_NODE
	} else {
		node.state = FOLLOWER_NODE
	}

}

func (node *Node) GetCommitIndex() interface{} {
	return node.commitIndex.GetCommitIndex()
}

// sendPeerMessagesLoop is responsible for sending messages to the given peer
func (node *Node) sendPeerMessagesLoop(peer *Peer) {
	var command Command

	for command = range peer.sendMessage {
		switch command.Action {
		// Shutdown our loop
		case ACTION_SHUTDOWN:
			close(peer.sendMessage)
			command.ResultChannel <- struct{}{}
		// Send request to vote.
		case ACTION_SEND_VOTE:
			args := &RequestVoteArgs{Topic: node.topic, Term: node.getTerm(), CandidateID: node.name}
			var err error
			args.LastLogIndex, args.LastLogTerm, err = node.log.LastLogEntryInfo()
			reply := &RequestVoteResults{}
			if err == nil {
				node.node_log("Sending vote request to peer %v\n", peer.Name)
				err = node.server.SendPeerMessage(peer.Name, "RPCHandler.RequestVote", args, reply)
				if err != nil {
					node.node_log("Error (%v) in RPC call to vote to peer %v.\n", err, peer.Name)
				} else {
					command.ResultChannel <- reply
				}
			} else {
				// Error - unable to make the call due to last log entry failing - shutdown
				node.server.RequestShutdown("Error getting last log entry")
			}
		// Send the latest log position.
		case ACTION_SEND_LOG, ACTION_SEND_ACK:
			args := &AppendEntriesArgs{Topic: node.topic, Term: node.getTerm(), LeaderID: node.name}
			reply := &AppendEntriesResults{}
			var err error
			tryingToAppend := true
			peerNextIndexStart := peer.GetNextIndex()
			for tryingToAppend && node.getState() == LEADER_NODE {
				args.LeaderCommitIndex = node.commitIndex.GetCommitIndex()
				// Use our understanding of the peer's nextIndex to determine what to send
				args.PrevLogIndex, args.PrevLogTerm, args.Entries, err = node.log.Retrieve(peerNextIndexStart)
				//node.node_log("Retrieve brought back: prev log index of %v, prev log term of %v, entries of %v for peerNextIndexStart of %v\n", args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), peerNextIndexStart)
				//node.node_log("Args for sending have entries: %v\n", args.Entries)
				if err == nil {
					if args.PrevLogIndex == 0 {
						// We are sending the first ever messages - lookup firstIndex
						args.LeaderFirstIndex, err = node.log.FirstIndex()
						if err != nil {
							node.server.RequestShutdown("Error getting first log index")
							return
						}
					}

					//node.node_log("Sending log entries %v to peer %v\n", args.entries, peer.Name)
					//node.node_log("Sending append log request to peer %v\n", peer.Name)
					//node.node_log("Routine (%v) - Think peer %v needs index %v onwards, sending previous index %v and total messages %v\n", ourGoRoutineID, peer.Name, peerNextIndexStart, args.PrevLogIndex, args.Entries.GetCount())
					err = node.server.SendPeerMessage(peer.Name, "RPCHandler.AppendEntries", args, reply)
					if err != nil {
						node.node_log("Error (%v) in RPC call to append log to peer %v.\n", err, peer.Name)
					} else {
						if reply.Success {
							tryingToAppend = false
							if args.Entries.GetCount() > 0 {
								var lastReplicatedMsgID int64
								if args.PrevLogIndex == 0 {
									lastReplicatedMsgID = args.LeaderFirstIndex + int64(args.Entries.GetCount()) - 1
								} else {
									lastReplicatedMsgID = args.PrevLogIndex + int64(args.Entries.GetCount())
								}
								//node.node_log("Peer accepted logs of length %v - last replicated ID is %v\n", len(args.Entries), lastReplicatedMsgID)
								peer.SetValidatedIndex(lastReplicatedMsgID, lastReplicatedMsgID+1)
								// If replicating these messages may have moved our commit forward, check.
								//node.node_log("Last replicated message ID %v, our commit index at start: %v\n", lastReplicatedMsgID, args.LeaderCommitIndex)
								if lastReplicatedMsgID > args.LeaderCommitIndex {
									//node.node_log("Re-calculating commit index.\n")
									node.commitIndex.UpdateCommitIndex(node)
									node.log.Commit(node.commitIndex.GetCommitIndex())
									//node.node_log("New commit index: %v\n", node.commitIndex.GetCommitIndex())
								}
								//If this node has fallen behind, try and catch up.
								ourLastIndex, err := node.log.LastIndex()
								if err != nil {
									node.server.RequestShutdown("Error getting last log entry info in append log send")
									return
								}
								if ourLastIndex > lastReplicatedMsgID {
									// Keep looping
									//node.node_log("Peer is behind - keep looping\n")
									tryingToAppend = true
									peerNextIndexStart = lastReplicatedMsgID + 1
								}
							} else {
								// We sent no messages, but we have a valid ACK.  If commit index is 0, update the last known good index
								if args.LeaderCommitIndex == 0 {
									peer.SetValidatedIndex(args.PrevLogIndex, args.PrevLogIndex+1)
									// See if we can commit this topic with a super-majority
									node.commitIndex.UpdateCommitIndex(node)
									node.log.Commit(node.commitIndex.GetCommitIndex())
								}
							}
						} else if reply.Term > node.getTerm() {
							// We are not the true leader - fallback to follower status.
							node.setState(FOLLOWER_NODE)
						} else if peerNextIndexStart > 1 {
							node.node_log("Peer rejected messages, searching backwards to find common ground.\n")
							// If the peer has a lower nextIndex, jump backwards
							if reply.NextIndex < peerNextIndexStart {
								node.node_log("Follower is expecting index %v, jumping backwards to this index\n", reply.NextIndex)
								peerNextIndexStart = reply.NextIndex
							} else {
								peerNextIndexStart--
							}
						} else {
							// Run out of messages and we still can't make it work - error
							node.node_log("Peer not accepting any messages - even from the start of the log\n")
							tryingToAppend = false
						}
					}
				} else {
					// Error - unable to make the call due to last log entry failing - shutdown
					node.server.RequestShutdown("Error getting last log entry")
					tryingToAppend = false
				}
			}
		}
	}
}

// holdElection is run in the ElectionTimer goroutine and is responsible for holding an election when the timeout has happened.
func (node *Node) holdElection() {
	victory := false
	node.lock.Lock()
	// Check we are still a follower
	if node.state == FOLLOWER_NODE {
		node.node_log("Starting election in term %v\n", node.currentTerm)
		// Increment our term and vote for ourselves
		node.currentTerm++
		curTerm := node.currentTerm
		node.votedFor = node.name
		// Save it.
		node.store.SetTerm(node.currentTerm, node.votedFor)
		// Change state
		node.state = CANDIDATE_NODE
		// Trigger RequestVotes to all peers
		peerCount := len(node.peers)
		results := make(chan interface{}, peerCount)
		command := Command{Action: ACTION_SEND_VOTE, ResultChannel: results}
		for _, peer := range node.peers {
			peer.SendMessage(command)
		}
		// Need to unlock at this point so that incoming AppendEntries can move us out of candidate mode
		node.lock.Unlock()
		voteCount := 1
		timer := time.NewTimer(CANDIDATE_VOTE_TIMEOUT)
		running := true
		for running {
			select {
			case voteResult := <-results:
				votingResults := voteResult.(*RequestVoteResults)
				if votingResults.VoteGranted {
					voteCount++
				}
				if votingResults.Term > curTerm {
					node.setTerm(votingResults.Term)
				}
			case <-timer.C:
				running = false
			}
		}
		node.node_log("Election results - %v votes out of %v nodes for term %v\n", voteCount, peerCount+1, node.getTerm())
		if float32(voteCount) > float32(peerCount+1)/2.0 {
			// We won the vote!  Now check it's still relevant
			node.node_log("Vote won - becoming leader.\n")
			node.lock.Lock()
			if node.state == CANDIDATE_NODE {
				// Still a candidate, so accept our nomination.
				node.state = LEADER_NODE
				victory = true
				// This initialisation has to happen here while we still have a lock to avoid
				// race conditions with the client goroutines
				// Start by noting which index is the minimum of this term for the commit logic.
				lastIndex, err := node.log.LastIndex()
				if err != nil {
					node.node_log("Error determining log entry info following vote: %v\n", err)
					node.server.RequestShutdown("Error determining log entry info following vote")
				}
				nextIndex := lastIndex + 1
				node.commitIndex.FirstIndexOfTerm(nextIndex, node.log.GetCurrentCommit())
				// Set the initial value of nextIndex for each peer to last log index + 1
				node.node_log("Setting peers to nextIndex of %v\n", nextIndex)
				for _, peer := range node.peers {
					peer.SetNextIndex(nextIndex)
					peer.SetLastIndex(lastIndex)
				}
			} else {
				node.node_log("No longer in candidate state, abandoning leadership for this term.\n")
			}
			node.lock.Unlock()
		} else {
			// We lost the election - go back to being a follower.
			node.state = FOLLOWER_NODE
		}
	} else {
		node.lock.Unlock()
	}
	// Did we win?
	if victory {
		node.leaderLoop()
	}
}

// leaderLoop is called from holdElection if we are elected leader.
// It runs on the electionTimer goroutine.
func (node *Node) leaderLoop() {
	// Tell everyone about our glorious victory
	ackCommand := Command{Action: ACTION_SEND_ACK}
	node.lock.RLock()
	for _, peer := range node.peers {
		peer.SendMessage(ackCommand)
	}
	node.lock.RUnlock()

	// Now run a loop that, while we are leader, ensures that followers recieve ACKs
	// And also keep up with any new log data
	ackTimer := time.NewTimer(LEADER_ACK_TIMEOUT)
	for node.getState() == LEADER_NODE {
		<-ackTimer.C
		// Periodic ack to keep followers aware of who's the leader
		// TODO: Optimise by suppressing acks for followers that have seen logs recently
		//node.node_log("Sending acks to all peers\n")
		node.lock.RLock()
		for _, peer := range node.peers {
			peer.SendMessage(ackCommand)
		}
		node.lock.RUnlock()
		ackTimer.Reset(LEADER_ACK_TIMEOUT)
	}
	// Let any waiting clients know that we are no longer the leader.
	node.commitIndex.ReleaseWaitingClients()
	node.node_log("Leadership loop ending - no longer the leader node\n")
}

/*
SendLogsToPeers is used by the QueueWriteAggregator to inform all peer goroutines that there are new log entries available.

If there are no peers (single node cluster) then this method triggers the commit index update so that messages are available to clients.
*/
func (node *Node) SendLogsToPeers() {
	ackCommand := Command{Action: ACTION_SEND_LOG, ResultChannel: nil}
	noPeers := true
	node.lock.RLock()
	for _, peer := range node.peers {
		noPeers = false
		peer.SendMessage(ackCommand)
	}
	node.lock.RUnlock()
	// If we have no peers, recalculate the commit index using just our change in log.
	if noPeers {
		node.commitIndex.UpdateCommitIndex(node)
		node.log.Commit(node.commitIndex.GetCommitIndex())
	}
}

// RequestVote handles all logic for dealing with a request to vote by another peer.
func (node *Node) RequestVote(args *RequestVoteArgs, results *RequestVoteResults) {
	node.lock.Lock()
	defer node.lock.Unlock()

	if node.state == SHUTDOWN_NODE {
		return
	}

	node.node_log("Received request during our term %v for vote from %v (term %v)\n", node.currentTerm, args.CandidateID, args.Term)
	results.Term = node.currentTerm
	// Default is a no vote
	results.VoteGranted = false

	// Note the activity to stop us triggering an election.
	node.electionTimer.Activity()

	if node.state == ORPHAN_NODE {
		node.node_log("Not voting as we are not part of the cluster.\n")
		return
	}

	// If the candidate is on an older term, vote no.
	if args.Term < node.currentTerm {
		node.node_log("Vote request: candidate has term %v, we are term %v - rejecting.\n", args.Term, node.currentTerm)
		return
	} else if args.Term > node.currentTerm {
		node.node_log("We had an old term %v, candidate is term %v\n", node.currentTerm, args.Term)
		// The term of the candidate is higher than our current term
		node.currentTerm = args.Term
		node.votedFor = ""
		err := node.store.SetTerm(node.currentTerm, node.votedFor)
		if err != nil {
			node.node_log("Error persisting term during handling of RequestVote: %v\n", err)
			node.server.RequestShutdown("Error persisting term during handling of RequestVote")
			return
		}
		// As we are changing term, we should also revert back to follower state
		node.state = FOLLOWER_NODE
	}
	if node.state == LEADER_NODE {
		node.node_log("We are currently leader and have an OK term (%v) - reject the vote request.\n", node.currentTerm)
		return
	}
	// If we have the same term then see whether we've voted
	node.node_log("Deciding on vote.  Current state - voted for: %v in term %v\n", node.votedFor, node.currentTerm)
	if node.votedFor == "" || node.votedFor == args.CandidateID {
		node.node_log("Voting for candidate %v in term %v.\n", args.CandidateID, node.currentTerm)
		// Either we've not voted yet in this term, or we've previously voted for this candidate
		// Does the candidate have logs at least as up-to-date as ours?
		lastIndex, lastTerm, err := node.log.LastLogEntryInfo()
		if err == nil {
			if args.LastLogIndex >= lastIndex && args.LastLogTerm >= lastTerm {
				// We can vote for them!
				results.VoteGranted = true
				node.votedFor = args.CandidateID
				// Persist who we voted for.
				err := node.store.SetTerm(node.currentTerm, node.votedFor)
				if err != nil {
					node.node_log("Error persisting term during handling of RequestVote: %v\n", err)
					node.server.RequestShutdown("Error persisting term during handling of RequestVote")
					return
				}
			}
		} else {
			node.node_log("Error during request vote handling: %v\n", err)
			node.server.RequestShutdown("Error during request vote handling")
		}
	}
}

// AppendEntries handles all logic for dealing with a leaders request to append entries
func (node *Node) AppendEntries(args *AppendEntriesArgs, results *AppendEntriesResults) {
	node.lock.Lock()
	defer node.lock.Unlock()

	if node.state == SHUTDOWN_NODE {
		return
	}

	//node.node_log("Recieved AppendEntries with message content of %v\n", args.Entries)

	// Note the activity to stop us triggering an election.
	//node.node_log("Recieved AppendEntries - ressetting election timer.\n")
	// Stop our election timer incase the append takes too long.
	node.electionTimer.Pause()
	defer node.electionTimer.Start()

	results.Term = node.currentTerm

	// If the request to append is from a leader in the past, refuse to append.
	if args.Term < node.currentTerm {
		node.node_log("Rejecting append as message term is %v, our term is %v\n", args.Term, node.currentTerm)
		results.Success = false
		return
	} else if args.Term > node.currentTerm {
		//node.node_log("Term from leader is ahead of our term, making sure we are in follower state\n")
		node.currentTerm = args.Term
		node.votedFor = ""
		err := node.store.SetTerm(node.currentTerm, node.votedFor)
		if err != nil {
			node.node_log("Error persisting term during handling of AppendEntries: %v\n", err)
			node.server.RequestShutdown("Error persisting term during handling of AppendEntries")
			return
		}

		// Switch back to follower if we are currently in candidate state
		if node.state != ORPHAN_NODE {
			node.state = FOLLOWER_NODE
		}
	} else if node.state != ORPHAN_NODE {
		// Terms must be equal
		node.state = FOLLOWER_NODE
	}
	node.leaderNode = args.LeaderID

	//node.node_log("AppendEntires arguments recieved: %v\n", args)

	ourNextID, previousMatch, err := node.log.Append(args.Entries, args.PrevLogIndex, args.PrevLogTerm, args.LeaderFirstIndex)
	if err != nil {
		node.node_log("Error appending entries into node - shutting down: %v", err)
		node.server.RequestShutdown("Error appending entries to log")
		return
	}

	results.Success = previousMatch
	results.NextIndex = ourNextID

	node.log.Commit(args.LeaderCommitIndex)

}

// Getters and Setters on the Node that honour the lock

// Term
func (node *Node) getTerm() int64 {
	node.lock.RLock()
	defer node.lock.RUnlock()
	return node.currentTerm
}

func (node *Node) setTerm(term int64) {
	node.lock.Lock()
	defer node.lock.Unlock()
	if term > node.currentTerm {
		node.currentTerm = term
		node.votedFor = ""
		node.store.SetTerm(term, node.votedFor)

	}
}

// VotedFor
func (node *Node) getVotedFor() string {
	node.lock.RLock()
	defer node.lock.RUnlock()
	return node.votedFor
}

// Leader
func (node *Node) getLastSeenLeader() string {
	node.lock.RLock()
	defer node.lock.RUnlock()
	return node.leaderNode
}

// State
func (node *Node) getState() NodeState {
	node.lock.RLock()
	defer node.lock.RUnlock()
	return node.state
}

func (node *Node) setState(newState NodeState) {
	node.lock.Lock()
	defer node.lock.Unlock()
	node.state = newState
}

// GetCommitLog doesn't apply the lock because the CommitLog doens't change over the lifetime of the Node.
func (node *Node) GetCommitLog() *commitlog.CommitLog {
	return node.log
}
