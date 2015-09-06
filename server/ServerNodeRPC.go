package server

import (
	"github.com/owlfish/forestbus-server/model"
	"github.com/owlfish/forestbus/rapi"
)

// RPCHandler holds all exposed RPC methods.
type RPCHandler struct {
	server     *ServerNode
	identified bool
	peerName   string
}

func NewRPCHandler(srv *ServerNode) *RPCHandler {
	return &RPCHandler{server: srv}
}

// IdentifyNodeArgs contains the arguments for a Node calling identify
type IdentifyNodeArgs struct {
	// Name is the name of the peer
	Name string
	// ClusterID is the ID the peer has been configured with.
	ClusterID string
}

type IdentifyNodeResults struct {
	Result rapi.ResultInfo
}

/*
IdentifyNode is used by other peers in the cluster to identify themselves to this node.
*/
func (handler *RPCHandler) IdentifyNode(args *IdentifyNodeArgs, results *IdentifyNodeResults) error {
	handler.peerName = args.Name
	handler.identified = true
	if args.ClusterID == handler.server.GetClusterID() {
		results.Result = rapi.Get_RI_SUCCESS()
	} else {
		results.Result = rapi.Get_RI_MISMATCHED_CLUSTER_ID(handler.server.address, args.ClusterID, handler.server.GetClusterID())
	}
	return nil
}

type RequestVoteArgs struct {
	Topic        string
	Term         int64
	CandidateID  string
	LastLogIndex int64
	LastLogTerm  int64
}

type RequestVoteResults struct {
	Term        int64
	VoteGranted bool
}

/*
RequestVote is used by peers to request a vote during an election.
*/
func (handler *RPCHandler) RequestVote(args *RequestVoteArgs, results *RequestVoteResults) error {
	if handler.identified {
		node := handler.server.GetNode(args.Topic)
		if node != nil {
			node.RequestVote(args, results)
		} else {
			return rapi.ERR_TOPIC_NOT_FOUND
		}
	}
	return nil
}

type AppendEntriesArgs struct {
	Topic        string
	Term         int64
	LeaderID     string
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      model.Messages
	// LeaderFirstIndex is set to the first index that the leader knows abuot.  Used when PreLogIndex is set to 0
	LeaderFirstIndex  int64
	LeaderCommitIndex int64
}

// AppendEntriesResults contains the returns data from a call by a leader to a follower to append messages
type AppendEntriesResults struct {
	// Term is the term of the follower node
	Term int64
	// Success is true if the append worked
	Success bool
	// NextIndex contains the next index that the follower is expecting.
	// If Success if false this can be used by the leader to determine whether to skip back in time
	// to allow faster catchup of new / recovering followers.
	NextIndex int64
}

/*
AppendEntries is used by leaders to inform followers of new message entries.
*/
func (handler *RPCHandler) AppendEntries(args *AppendEntriesArgs, results *AppendEntriesResults) error {
	if handler.identified {
		node := handler.server.GetNode(args.Topic)
		if node != nil {
			node.AppendEntries(args, results)
		} else {
			return rapi.ERR_TOPIC_NOT_FOUND
		}
	}
	return nil
}

type ConfigChangeResults struct {
	Result rapi.ResultInfo
}

/*
ConfigurationChange is used by the admin tool to send new configuration details to the nodes.
*/
func (handler *RPCHandler) ConfigurationChange(args *ServerNodeConfiguration, results *ConfigChangeResults) error {
	handler.server.ChangeConfiguration(args, results)
	return nil
}

// Client RPC support

/*
SendMessages is a Client RPC method allowing clients to send new messages to a topic on the leader node.
*/
func (handler *RPCHandler) SendMessages(args *rapi.SendMessagesArgs, results *rapi.SendMessagesResults) error {
	node := handler.server.GetNode(args.Topic)
	if node != nil {
		return node.ClientRequestSendMessages(args, results)
	} else {
		results.Result = rapi.Get_RI_TOPIC_NOT_FOUND(handler.server.address, args.Topic)
		return nil
	}
}

/*
ReceiveMessages is a Client RPC method allowing clients to get messages from this node.
*/
func (handler *RPCHandler) ReceiveMessages(args *rapi.ReceiveMessagesArgs, results *rapi.ReceiveMessagesResults) error {
	node := handler.server.GetNode(args.Topic)
	if node != nil {
		return node.ClientReceiveMessages(args, results)
	} else {
		return rapi.ERR_TOPIC_NOT_FOUND
	}
}

/*
GetClusterDetails is a Client RPC method allowing clients to discover details about the cluster configuration.
*/
func (handler *RPCHandler) GetClusterDetails(args *rapi.GetClusterDetailsArgs, results *rapi.GetClusterDetailsResults) error {
	return handler.server.GetClusterDetails(args, results)
}

/*
GetTopicDetails is a Client RPC method allowing clients to discover details about a topic.
*/
func (handler *RPCHandler) GetTopicDetails(args *rapi.GetTopicDetailsArgs, results *rapi.GetTopicDetailsResults) error {
	node := handler.server.GetNode(args.Topic)
	if node != nil {
		return node.GetTopicDetails(args, results)
	} else {
		results.Result = rapi.Get_RI_TOPIC_NOT_FOUND(handler.server.address, args.Topic)
		return nil
	}
}
