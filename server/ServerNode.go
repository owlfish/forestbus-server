/*
The server package contains the networking and coordination logic for the Forest Bus server implementation.

There are several different objects used to implement the server logic.

	ServerNode - This is the root object managing the overall state and coordination of a Forest Bus Server.
	RPCHandler - Entry point for RPC requests into this server.
	ServerNodeConfiguration - This holds configuration of the overall server.
	Node - This is the main Topic object that implements the Raft algorithm.

For each topic (Node) additional objects are used.

	RaftElectionTimer - Manages the leadership timeouts.
	QueueWriteAggregator - Consolidates individual batches of messages from clients for efficient writing to the commitlog.
	Peer - Represents the topic's state on other peers.
*/
package server

import (
	"errors"
	"github.com/owlfish/forestbus-server/commitlog"
	"github.com/owlfish/forestbus-server/disklog"
	"github.com/owlfish/forestbus-server/utils"
	"github.com/owlfish/forestbus/rapi"
	"github.com/ugorji/go/codec"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sort"
	"sync"
	"time"
)

// default_peer_connections_count is the number of connections each node in the cluster will establish with it's peers.
const default_peer_connections_count = 10

// NODE_CONNECTION_INTERVAL is the amount of time to wait between connection attempts.
const NODE_CONNECTION_INTERVAL = time.Second * 2

// SHUTDOWN_WAIT_TIMER is the amount of time to wait for a clean shutdown of the server.
const SHUTDOWN_WAIT_TIMER = time.Second * 5

// ERR_PEER_NOT_FOUND is returned when a node connects that is not a known peer.
var ERR_PEER_NOT_FOUND = errors.New("Unknown peer")

// ERR_NO_DATA_PATHS is returned when no data storage paths are found.
var ERR_NO_DATA_PATHS = errors.New("No data paths defined")

// ERR_NO_PEER_CONNECTION_AVAILABLE is returned when there is no connection available for a given peer.
var ERR_NO_PEER_CONNECTION_AVAILABLE = errors.New("Peer connection not available.")

// srv_log is the log.Printf instance used by the server.
var srv_log = utils.GetServerLogger("Server")

// A ConfigChangeRequest is used internally within the server to serialise the application of new ServerNodeConfiguration
type ConfigChangeRequest struct {
	Configuration *ServerNodeConfiguration
	Response      chan rapi.ResultInfo
}

// ServerNodeInfo is used to provide information on the server to ExpVar.
type ServerNodeInfo struct {
	Name           string
	ServerPeerInfo []interface{}
	TopicsInfo     []interface{}
}

/*
A ServerNode is the main object of the server.  It holds the master of the configuration, peer connections and topics.
*/
type ServerNode struct {
	topics map[string]*Node
	// topicNameList is the list of topic names sorted alphabetically.
	topicNameList  []string
	address        string
	gobAddress     string
	cborAddress    string
	httpAddress    string
	lock           sync.RWMutex
	peers          map[string]*ServerPeer
	config         *ServerNodeConfiguration
	shutdownServer chan string
	changeConfig   chan ConfigChangeRequest
	inShutdown     bool
	rpcListener    net.Listener
	httpListener   net.Listener
	cborListener   net.Listener
	gobListener    net.Listener
}

/*
The ServerPeer is used by the ServerNode to manage connections to a peer.
*/
type ServerPeer struct {
	ourName            string
	name               string
	connections        chan *rpc.Client
	broken_connections chan interface{}
	shutdown_channel   chan *utils.ShutdownNotifier
}

// ServerPeerInfo is used for ExpVar reporting on the state of Peer connnections.
type ServerPeerInfo struct {
	Name                  string
	ConnectionCount       int
	BrokenConnectionCount int
}

/*
NewServerPeer creates a new ServerPeer to hold connections to a peer node.
*/
func NewServerPeer(ourname, name string) *ServerPeer {
	peer := &ServerPeer{ourName: ourname, name: name}
	peer.connections = make(chan *rpc.Client, default_peer_connections_count)
	peer.broken_connections = make(chan interface{}, default_peer_connections_count)
	peer.shutdown_channel = make(chan *utils.ShutdownNotifier, 1)
	return peer
}

/*

 */
func (peer *ServerPeer) manageOutboundConnections(clusterID string) {
	var notifier *utils.ShutdownNotifier
	for {
		select {
		case notifier = <-peer.shutdown_channel:
			// Channel has been closed - shutdown all connections
			close(peer.connections)
			for conn := range peer.connections {
				conn.Close()
			}
			if notifier != nil {
				notifier.ShutdownDone()
			}
			return
		case <-peer.broken_connections:
			srv_log("Handling request to establish connection to peer %v\n", peer.name)
			recentConnectionAttempt := true
			connected := false
			for !connected {
				//rpcclient, err := rpc.Dial("tcp", peer.name)
				netconn, err := net.Dial("tcp", peer.name)
				if err != nil {
					if recentConnectionAttempt {
						srv_log("Unable to connect to peer %v (%v) - will keep trying periodically\n", peer.name, err)
						recentConnectionAttempt = false
					}
				} else {
					rpcclient := rpc.NewClient(netconn)

					recentConnectionAttempt = true
					srv_log("Connection to peer %v established!\n", peer.name)
					// Identify ourselves to our peer
					args := &IdentifyNodeArgs{Name: peer.ourName, ClusterID: clusterID}
					reply := &IdentifyNodeResults{}
					srv_log("Sending identify request to peer %v\n", peer.name)
					err = rpcclient.Call("RPCHandler.IdentifyNode", args, reply)
					if err != nil {
						srv_log("Error in RPC call (%v) for identify to peer %v - disconnecting.\n", err, peer.name)
						rpcclient.Close()
					} else if reply.Result.Code != rapi.RI_SUCCESS {
						srv_log("Identify call to peer failed: %v\n", reply.Result.Description)
						rpcclient.Close()
					} else {
						// Now we are connected, service any send messages
						connected = true
						srv_log("Identity sent - serving outbound requests\n")
						peer.connections <- rpcclient
					}
				}
				// Wait before trying again
				if !connected {
					time.Sleep(NODE_CONNECTION_INTERVAL)
				}
			}
		}
	}
}

func (peer *ServerPeer) shutdown(notifier *utils.ShutdownNotifier) {
	peer.shutdown_channel <- notifier
}

func (peer *ServerPeer) ExpVar() interface{} {
	stats := &ServerPeerInfo{}
	stats.Name = peer.name
	stats.ConnectionCount = len(peer.connections)
	stats.BrokenConnectionCount = len(peer.broken_connections)
	return stats
}

func NewServerNode(address, gobAddress, httpAddress, cborAddress, rootpath, cluster_id string) (*ServerNode, error) {
	var err error
	srv := &ServerNode{}
	srv.peers = make(map[string]*ServerPeer)
	srv.shutdownServer = make(chan string)
	srv.changeConfig = make(chan ConfigChangeRequest)
	srv.address = address
	srv.gobAddress = gobAddress
	srv.httpAddress = httpAddress
	srv.cborAddress = cborAddress
	srv.config, err = LoadConfiguration(cluster_id, rootpath)
	if err != nil {
		srv_log("Error loading configuration file: %v\n", err)
		return nil, err
	} else {
		srv_log("Configuration file loaded.")
	}
	srv.setPeers(srv.config.Peers)

	srv.topics = make(map[string]*Node)
	// Load topics
	err = srv.loadExistingTopics()
	if err != nil {
		srv_log("Error loading existing topics: %v\n", err)
		return nil, err
	}

	// Topics loaded - start them running
	for _, topicNode := range srv.topics {
		topicNode.StartNode()
	}

	// Start config change goroutine.
	go srv.manageConfigurationChanges()
	return srv, nil
}

func (srv *ServerNode) manageConfigurationChanges() {
	srv_log("Waiting for configuration requests to handle\n")
	for request := range srv.changeConfig {
		srv_log("Processing new configuration change request\n")
		newConfig := request.Configuration
		if newConfig.Cluster_ID == srv.config.Cluster_ID {
			var err error
			srv_log("Recieved new configuration")
			if newConfig.Scope&CNF_Set_Peers != 0 {
				srv_log("Change in peers configuration")
				srv.config.Peers = newConfig.Peers
				srv.setPeers(newConfig.Peers)
				var peerConfigToUse ConfigPeers
				// Use the new configuration for topics only if we are part of the cluster.
				if newConfig.Peers.Contains(srv.address) {
					peerConfigToUse = newConfig.Peers
				} else {
					peerConfigToUse = make(ConfigPeers, 0)
				}
				srv_log("Passing new peer configuration to topics.\n")
				for _, topic := range srv.topics {
					topic.ChangePeerConfiguration(peerConfigToUse)
				}
			} else if newConfig.Scope&CNF_Set_Topic != 0 {
				srv_log("Topic configuration")
				for _, newTopic := range newConfig.Topics {

					if !srv.config.Topics.Contains(newTopic.Name) {
						topicConfig := GetDefaultTopicConfiguration()
						topicConfig.Name = newTopic.Name
						if newTopic.SegmentSize > 0 {
							topicConfig.SegmentSize = newTopic.SegmentSize
						}
						srv.config.Topics[newTopic.Name] = topicConfig
						node, err := srv.createTopic(topicConfig)
						node.StartNode()
						if err != nil {
							break
						}
					} else {
						// Modification of existing topic.
						configChangesToApply := make([]disklog.DiskLogConfigFunction, 0, 3)
						currentTopic := srv.config.Topics[newTopic.Name]
						if newTopic.SegmentSize > 0 && currentTopic.SegmentSize != newTopic.SegmentSize {
							// Change the current configuration
							currentTopic.SegmentSize = newTopic.SegmentSize
							configChangesToApply = append(configChangesToApply, disklog.SetTargetSegmentSize(newTopic.SegmentSize))
						}
						if newTopic.SegmentCleanupAge >= 0 && currentTopic.SegmentCleanupAge != newTopic.SegmentCleanupAge {
							// Change the current configuration
							currentTopic.SegmentCleanupAge = newTopic.SegmentCleanupAge
							configChangesToApply = append(configChangesToApply, disklog.SetSegmentCleanupAge(newTopic.SegmentCleanupAge))
						}
						if len(configChangesToApply) > 0 {
							// Store any changes we've made
							srv.config.Topics[newTopic.Name] = currentTopic
							// Update the current configuration.
							srv.lock.RLock()
							node := srv.topics[newTopic.Name]
							diskStorage := node.GetCommitLog().GetLogStorage().(*disklog.DiskLogStorage)
							diskStorage.ChangeConfiguration(configChangesToApply...)
							srv.lock.RUnlock()
						}

					}
				}
			} else if newConfig.Scope&CNF_Remove_Topic != 0 {
				srv_log("Topic removal")
				for _, removedTopic := range newConfig.Topics {
					if srv.config.Topics.Contains(removedTopic.Name) {
						currentTopic := srv.config.Topics[removedTopic.Name]
						delete(srv.config.Topics, removedTopic.Name)
						err = srv.removeTopic(currentTopic)
						if err != nil {
							break
						}
					} else {
						srv_log("Topic %v not present - no removal required.\n", removedTopic.Name)
					}
				}
			}
			if err == nil {
				srv_log("Saving configuration\n")
				err = srv.config.SaveConfiguration()
			}

			if err != nil {
				request.Response <- rapi.Get_RI_INTERNAL_ERROR(srv.address, err.Error())
			} else {
				request.Response <- rapi.Get_RI_SUCCESS()
			}
		} else {
			srv_log("Cluster ID give for config change (%v) doesn't match server cluster ID of (%v)\n", newConfig.Cluster_ID, srv.config.Cluster_ID)
			request.Response <- rapi.Get_RI_MISMATCHED_CLUSTER_ID(srv.address, newConfig.Cluster_ID, srv.config.Cluster_ID)
		}
	}
	srv_log("manageConfigurationChanges shutdown\n")
}

func (srv *ServerNode) ChangeConfiguration(newconfig *ServerNodeConfiguration, results *ConfigChangeResults) {
	response := make(chan rapi.ResultInfo)
	request := ConfigChangeRequest{Configuration: newconfig, Response: response}
	srv_log("Queueing configuration change\n")
	srv.changeConfig <- request
	srv_log("Waiting for configuration change result\n")
	results.Result = <-request.Response
	srv_log("Configuration change complete.\n")
}

/*
loadExistingTopics looks for topics within this servers data paths and loads them into the server.

Logic used:

	1 - Check that there is at least one data path.
	2 - For each data path:
	3 - Look for the list of directories in the data path
	4 - Check that the directory is in the list of configured Topics
	5 - Load the topic
	6 - Create any topics defined in the Topics list that were not found.
*/
func (srv *ServerNode) loadExistingTopics() error {
	if len(srv.config.Data_Paths) == 0 {
		srv_log("No data paths defined for this node\n")
		return ERR_NO_DATA_PATHS
	}
	var topicsLoaded = make(map[string]bool)
	for _, dataPath := range srv.config.Data_Paths {
		dataDirFile, err := os.Open(dataPath)
		defer dataDirFile.Close()
		if err != nil {
			srv_log("Error opening data path %v: %v\n", dataPath, err)
			return err
		}
		names, err := dataDirFile.Readdirnames(-1)
		if err != nil {
			srv_log("Error reading topic directories in data path %v: %v\n", dataPath, err)
			return err
		}
		for _, topicName := range names {
			if srv.config.Topics.Contains(topicName) {
				_, err = srv.loadTopic(dataPath, srv.config.Topics[topicName])
				if err != nil {
					srv_log("Error loading topic %v from data path %v: %v\n", topicName, dataPath, err)
					return err
				}
				topicsLoaded[topicName] = true
			} else {
				srv_log("WARNING: Found topic data %v in data path %v, but this is not a configured topic!\n", topicName, dataPath)
			}
		}
	}
	// Now see if there are any configured topics that we don't have
	for _, topic := range srv.config.Topics {
		_, ok := topicsLoaded[topic.Name]
		if !ok {
			srv_log("WARNING: Topic %v configured but not found - creating.\n", topic.Name)
			_, err := srv.createTopic(topic)
			if err != nil {
				srv_log("Error creating topic: %v\n", err)
				return err
			}
		}
	}
	return nil
}

func (srv *ServerNode) loadTopic(dataPath string, topic ConfigTopic) (*Node, error) {
	topicPath := path.Join(dataPath, topic.Name)
	dataLog := &commitlog.CommitLog{}
	topicStore := disklog.NewDiskTopicPersistentStore(topicPath)
	dataStore, err := disklog.LoadLog(topic.Name, topicPath, disklog.SetTargetSegmentSize(topic.SegmentSize), disklog.SetSegmentCleanupAge(topic.SegmentCleanupAge))
	if err != nil {
		srv_log("Error loading disk log: %v\n", err)
		return nil, err
	}

	err = dataLog.SetupLog(topic.Name, dataStore)
	if err != nil {
		srv_log("Error setting up log: %v\n", err)
		return nil, err
	}

	node := &Node{}
	var peerConfigToUse ConfigPeers
	if srv.config.Peers.Contains(srv.address) {
		peerConfigToUse = srv.config.Peers
	} else {
		peerConfigToUse = make(ConfigPeers, 0)
	}

	err = node.SetupNode(topic.Name, srv, srv.address, peerConfigToUse, dataLog, topicStore)
	if err != nil {
		srv_log("Error starting node, stopping.")
		return nil, err
	}
	// Adding this topic
	srv.lock.Lock()
	srv.topics[topic.Name] = node
	// Update the sorted list of topic names
	srv.updateTopicNameListHoldingLock()
	srv.lock.Unlock()
	srv_log("Topic %v loaded and ready to serve\n", topic.Name)
	return node, nil
}

func (srv *ServerNode) updateTopicNameListHoldingLock() {
	srv.topicNameList = make([]string, 0, len(srv.topics))
	for k := range srv.topics {
		srv.topicNameList = append(srv.topicNameList, k)
	}
	sort.Strings(srv.topicNameList)
}

func (srv *ServerNode) createTopic(topic ConfigTopic) (*Node, error) {
	topicDir := path.Join(srv.config.Data_Paths[len(srv.config.Data_Paths)-1], topic.Name)
	err := os.Mkdir(topicDir, os.ModePerm)
	if err != nil {
		if !os.IsExist(err) {
			srv_log("Error creating new topic directory %v: %v\n", topicDir, err)
			return nil, err
		} else {
			srv_log("Topic directory already exists - loading.")
		}
	}
	return srv.loadTopic(srv.config.Data_Paths[len(srv.config.Data_Paths)-1], topic)
}

/*
removeTopic shutdowns the currently running topic node and removes the topic from the configuration.
This does not delete any data.
*/
func (srv *ServerNode) removeTopic(topic ConfigTopic) error {
	// Remove the topic from our list
	srv.lock.Lock()
	node := srv.topics[topic.Name]
	delete(srv.topics, topic.Name)
	// Update the sorted list of topic names
	srv.updateTopicNameListHoldingLock()
	srv.lock.Unlock()
	// Ask the node to shutdown
	notifier := utils.NewShutdownNotifier(1)
	node.Shutdown(notifier)
	notifier.WaitForAllDone()
	srv_log("Topic %v removed - data can now be deleted.\n", topic.Name)
	return nil
}

/*
setPeers updaates the server configuration with the new set of peers.

Logic:

	1 - Determine the list of peers that are being removed
	2 - Determine the list of peers that are being added
	3 - For each removed peer, shutdown the connection management
	4 - Add new peers, setting up connection management
	5 - Set the new peer list on each topic.
*/
func (srv *ServerNode) setPeers(newPeerSet ConfigPeers) {
	srv.lock.Lock()
	defer srv.lock.Unlock()
	var peersToKeep = make(map[string]*ServerPeer)
	// Figure out which peers are going
	for peerName, existingPeer := range srv.peers {
		if !newPeerSet.Contains(peerName) {
			// Shutdown this peer
			existingPeer.shutdown(nil)
		} else {
			peersToKeep[peerName] = existingPeer
		}
	}
	if newPeerSet.Contains(srv.address) {
		// Figure out which peers are new
		for _, newPeerName := range newPeerSet.Excluding(srv.address) {
			if _, exists := srv.peers[newPeerName]; !exists {
				// New peer - append it.
				newPeerInst := NewServerPeer(srv.address, newPeerName)
				peersToKeep[newPeerName] = newPeerInst
				srv_log("Adding new peer %v\n", newPeerName)
				// Start a connection manager goroutine
				go newPeerInst.manageOutboundConnections(srv.config.Cluster_ID)
				// Start by establishing some outbound connections.
				for i := 0; i < default_peer_connections_count; i++ {
					newPeerInst.broken_connections <- struct{}{}
				}
			}
		}
	} else {
		srv_log("WARNING: Our listening address is not part of the peers set - suppressing connections.")
		// Shutdown any left over connections
		for _, existingPeer := range srv.peers {
			existingPeer.shutdown(nil)
		}
		peersToKeep = make(map[string]*ServerPeer)
	}
	// Replace the list of peers with the reduced set
	srv.peers = peersToKeep

}

// listenConnections starts monitoring of incoming calls to allow for peer connections
func (srv *ServerNode) ListenConnections() error {
	var err error
	srv.rpcListener, err = net.Listen("tcp", srv.address)
	if err != nil {
		return err
	}

	if srv.httpAddress != "" {
		srv.httpListener, err = net.Listen("tcp", srv.httpAddress)
		if err != nil {
			return err
		}
	}

	if srv.cborAddress != "" {
		srv.cborListener, err = net.Listen("tcp", srv.cborAddress)
		if err != nil {
			return err
		}
		go func() {
			running := true
			for running {
				conn, err := srv.cborListener.Accept()
				if err != nil {
					srv_log("Listener no longer able to accept - finishing.\n")
					running = false
				} else {
					// Start listening to RPC requests using CBOR encoding.
					var handle codec.CborHandle
					rpcCodec := codec.GoRpc.ServerCodec(conn, &handle)
					remoteName := conn.RemoteAddr().String()
					handler := NewRPCHandler(srv)
					RPCServer := rpc.NewServer()
					RPCServer.Register(handler)
					srv_log("Setup incoming connection from %v with RPC handler\n", remoteName)
					go func() {
						RPCServer.ServeCodec(rpcCodec)
						srv_log("Node connected from %v disconnected.\n", remoteName)
						conn.Close()
					}()
				}
			}
		}()
	}

	if srv.gobAddress != "" {
		srv.gobListener, err = net.Listen("tcp", srv.gobAddress)
		if err != nil {
			return err
		}
		go func() {
			running := true
			for running {
				conn, err := srv.gobListener.Accept()
				if err != nil {
					srv_log("Listener no longer able to accept - finishing.\n")
					running = false
				} else {
					// Start listening to RPC requests using GOB encoding.
					remoteName := conn.RemoteAddr().String()
					handler := NewRPCHandler(srv)
					RPCServer := rpc.NewServer()
					RPCServer.Register(handler)
					srv_log("Setup incoming connection from %v with RPC handler\n", remoteName)
					go func() {
						RPCServer.ServeConn(conn)
						srv_log("Node connected from %v disconnected.\n", remoteName)
						conn.Close()
					}()
				}
			}
		}()
	}

	go func() {
		running := true
		for running {
			conn, err := srv.rpcListener.Accept()
			if err != nil {
				srv_log("Listener no longer able to accept - finishing.\n")
				running = false
			} else {
				// Start listening to RPC requests so that the node can identify itself
				remoteName := conn.RemoteAddr().String()
				//var handle codec.CborHandle
				//rpcCodec := codec.GoRpc.ServerCodec(conn, &handle)
				handler := NewRPCHandler(srv)
				RPCServer := rpc.NewServer()
				RPCServer.Register(handler)
				srv_log("Setup incoming connection from %v with RPC handler\n", remoteName)
				go func() {
					//RPCServer.ServeCodec(rpcCodec)
					RPCServer.ServeConn(conn)
					srv_log("Node connected from %v disconnected.\n", remoteName)
					conn.Close()
				}()
			}
		}
	}()

	if srv.httpListener != nil {
		go func() {
			httpServer := &http.Server{Handler: CrossOriginReponseHandler(http.DefaultServeMux)}
			httpServer.Serve(srv.httpListener)
			srv_log("HTTP listener closed.")
		}()
	}
	return nil
}

/*
WaitForShutdown blocks until the server is asked to shutdown, then it orchestrates the shutdown.

The shutdown sequence is:

	- Close the RPC and HTTP listeners
	- Shutdown the configuration go-routine
	- Ask each topic's RaftNode to shutdown, this in turn:
		- Shut's down the election timer / leader loop
		- Ask's each peer goroutine to quit
		- Once each's peer's connection is closed, shutdown the write aggregator
		- Once the write aggregator is shutdown, notify the storage of shutdown.  Disk storage will:
			- Shutdown the cleanup goroutine
			- Shutdown the segment close goroutine
			- Close each open segment
	- Shutdown each ServerPeer connection
	- Close main goroutine

Note that open RPC connections from other nodes are not closed, but they will behave safely if any new requests are recieved.
*/
func (srv *ServerNode) WaitForShutdown() {
	reason := <-srv.shutdownServer
	if srv.inShutdown {
		srv_log("Received additional request to shutdown (%v), ignoring as we are in shutdown procedure.\n", reason)
		return
	}
	srv.inShutdown = true
	srv_log("Received request to shutdown: %v", reason)
	//srv_log("Number of goroutines to shutdown: %v\n", runtime.NumGoroutine())
	// Close the RPC and http listener
	srv_log("Shutting down incoming listeners.\n")
	srv.rpcListener.Close()
	if srv.httpListener != nil {
		srv.httpListener.Close()
	}
	if srv.cborListener != nil {
		srv.cborListener.Close()
	}
	if srv.gobListener != nil {
		srv.gobListener.Close()
	}
	srv_log("Shutting down configuration goroutine.\n")
	close(srv.changeConfig)

	// Trigger each raftNode shutdown
	srv.lock.RLock()
	topicsList := make([]*Node, 0, len(srv.topics))
	for _, node := range srv.topics {
		topicsList = append(topicsList, node)
	}
	srv.lock.RUnlock()
	notifier := utils.NewShutdownNotifier(len(topicsList))
	for _, topic := range topicsList {
		topic.Shutdown(notifier)
	}

	done := notifier.WaitForDone(SHUTDOWN_WAIT_TIMER)

	srv_log("Shutdown of %v out of %v topics completed\n", done, len(topicsList))

	srv.lock.Lock()
	peerCount := len(srv.peers)
	peerConnectionsNotifier := utils.NewShutdownNotifier(peerCount)
	for _, srvPeer := range srv.peers {
		srvPeer.shutdown(peerConnectionsNotifier)
	}
	srv.peers = make(map[string]*ServerPeer)
	srv.lock.Unlock()

	done = peerConnectionsNotifier.WaitForDone(SHUTDOWN_WAIT_TIMER)
	srv_log("Shutdown %v out of %v peer connections\n", done, peerCount)

	// Give chance for other goroutines to complete
	time.Sleep(500 * time.Millisecond)

	srv_log("Shutdown procedure complete.")
	return
}

func (srv *ServerNode) RequestShutdown(reason string) {
	srv.shutdownServer <- reason
}

func (srv *ServerNode) GetNode(topic string) *Node {
	srv.lock.RLock()
	defer srv.lock.RUnlock()
	return srv.topics[topic]
}

/*
GetClusterID returns the ClusterID that was set when the server started up.
Locking is not used as the value will not change after startup.
*/
func (srv *ServerNode) GetClusterID() string {
	return srv.config.Cluster_ID
}

func (srv *ServerNode) GetClusterDetails(args *rapi.GetClusterDetailsArgs, results *rapi.GetClusterDetailsResults) error {
	srv.lock.RLock()
	defer srv.lock.RUnlock()
	if srv.inShutdown {
		results.Result = rapi.Get_RI_NODE_IN_SHUTDOWN(srv.address)
		return nil
	}
	results.ClusterID = srv.config.Cluster_ID
	results.Peers = make([]string, 0, len(srv.config.Peers))
	for _, peerName := range srv.config.Peers {
		results.Peers = append(results.Peers, peerName)
	}
	results.Topics = make([]string, 0, len(srv.config.Topics))
	for topicName, _ := range srv.config.Topics {
		results.Topics = append(results.Topics, topicName)
	}
	results.Result = rapi.Get_RI_SUCCESS()
	return nil
}

func (srv *ServerNode) SendPeerMessage(peerName string, api string, args interface{}, reply interface{}) error {
	srv.lock.RLock()
	// Find the peer
	var peer *ServerPeer
	peer = srv.peers[peerName]
	srv.lock.RUnlock()

	if peer == nil {
		return ERR_PEER_NOT_FOUND
	}
	// Attempt to get a connection from the pool
	//srv_log("Looking for connection in pool for peer %v\n", peerName)
	var client *rpc.Client
	var ok bool
	client, ok = <-peer.connections
	if !ok {
		srv_log("Peer %v connection in shutdown - unable to send to peer\n", peerName)
		return ERR_PEER_NOT_FOUND
	}

	//srv_log("Found connection - sending api call %v\n", api)
	err := client.Call(api, args, reply)
	if err != nil {
		srv_log("Error in outbound call to %v - closing client and asking for a new connection: %v\n", peerName, err)
		client.Close()
		peer.broken_connections <- struct{}{}
	} else {
		//srv_log("Call worked - returning connection to the pool\n")
		// It worked - restore the connection to the pool
		peer.connections <- client
	}
	return err
}

// Returns stats for expvar
func (srv *ServerNode) ExpVar() interface{} {
	srv.lock.RLock()
	defer srv.lock.RUnlock()
	stats := &ServerNodeInfo{}
	stats.Name = srv.address
	stats.ServerPeerInfo = make([]interface{}, 0, len(srv.peers))
	for _, peer := range srv.peers {
		stats.ServerPeerInfo = append(stats.ServerPeerInfo, peer.ExpVar())
	}

	stats.TopicsInfo = make([]interface{}, 0, len(srv.topics))
	for _, topicName := range srv.topicNameList {
		stats.TopicsInfo = append(stats.TopicsInfo, srv.topics[topicName].ExpVar())
	}
	return stats
}

func CrossOriginReponseHandler(onwardsHandler http.Handler) (handler http.HandlerFunc) {
	return func(rw http.ResponseWriter, req *http.Request) {
		if origin := req.Header.Get("Origin"); origin != "" {
			rw.Header().Set("Access-Control-Allow-Origin", origin)
			rw.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
			rw.Header().Set("Access-Control-Allow-Headers",
				"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		}
		// Stop here if its Preflighted OPTIONS request
		if req.Method == "OPTIONS" {
			return
		}
		// Let the onwards handler deal with this.
		onwardsHandler.ServeHTTP(rw, req)
	}
}
