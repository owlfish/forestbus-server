package server

import (
	"github.com/owlfish/forestbus/rapi"
)

// ClientRequestSendMessages checks we are the leader and then queues the messages
// Results are only sent once the messages are on the queue
func (node *Node) ClientRequestSendMessages(args *rapi.SendMessagesArgs, results *rapi.SendMessagesResults) error {
	//log.Printf("Starting request send message\n")
	state := node.getState()
	if state == SHUTDOWN_NODE {
		results.Result = rapi.Get_RI_NODE_IN_SHUTDOWN(node.name)
		return nil
	} else if state != LEADER_NODE {
		results.Result = rapi.Get_RI_NODE_NOT_LEADER(node.name, node.getLastSeenLeader())
		return nil
	}
	//log.Printf("Sending to queue aggregator.\n")
	//IDs, err := node.log.Queue(node.getTerm(), args.SentMessages)
	IDs, err := node.writeAggregator.Queue(args.SentMessages)
	//log.Printf("Got response from write aggregator\n")
	results.IDs = IDs
	if err == nil {
		results.Result = rapi.Get_RI_SUCCESS()
		if !args.WaitForCommit {
			return nil
		}
		if len(IDs) > 0 {
			// See if we are waiting on commit
			waitForIndex := IDs[len(IDs)-1]
			for node.getState() == LEADER_NODE {
				if node.commitIndex.WaitOnCommitChange(waitForIndex) >= waitForIndex {
					return nil
				}
			}
			// If we get here then we are no longer the leader - return an error
			results.Result = rapi.Get_RI_NODE_NOT_LEADER(node.name, node.getLastSeenLeader())
		}
	}
	return err
}

// ClientReceiveMessages returns messages at and beyond ID
// If WaitForMessages is true and no messages are currently present, this method blocks
// until at least one message can be returned
func (node *Node) ClientReceiveMessages(args *rapi.ReceiveMessagesArgs, results *rapi.ReceiveMessagesResults) error {
	state := node.getState()
	if state == SHUTDOWN_NODE {
		results.Result = rapi.Get_RI_NODE_IN_SHUTDOWN(node.name)
		return nil
	}

	msgs, nextID, err := node.log.Get(args.ID, args.Quantity, args.WaitForMessages)

	if err == nil {
		results.Result = rapi.Get_RI_SUCCESS()
		results.ReceivedMessages = msgs
		results.NextID = nextID
	}
	return err

}

func (node *Node) GetTopicDetails(args *rapi.GetTopicDetailsArgs, results *rapi.GetTopicDetailsResults) error {
	state := node.getState()
	var err error
	if state == SHUTDOWN_NODE {
		results.Result = rapi.Get_RI_NODE_IN_SHUTDOWN(node.name)
		return nil
	}

	results.FirstIndex, err = node.log.FirstIndex()
	if err != nil {
		results.Result = rapi.Get_RI_INTERNAL_ERROR(node.name, err.Error())
		return nil
	}
	results.LastIndex, err = node.log.LastIndex()
	if err != nil {
		results.Result = rapi.Get_RI_INTERNAL_ERROR(node.name, err.Error())
		return nil
	}
	results.CommitIndex = node.log.GetCurrentCommit()
	results.Result = rapi.Get_RI_SUCCESS()
	return nil
}
