/*
forest-admin is the binary command for the administration interface for Forest Bus.

The forest-bus-server instances must be running for forest-admin to connect to.

Usage:

	forest-admin [-id <clusterID>] [-node node1[,...]] command [arguments]
When -id <clusterID> is specified without -node forest-admin reads the nodes to
configure from a file “<clusterID>.nodelist” that is created with the peers
command.

Adding -node sends commands to just the node(s) specified.

The commands are:

	peers		Sets the peer list for the given node or cluster ID.
	topic		Creates or updates the configuration of a given topic.

Use “forest-admin -help [command]” for more information about a command.

*/
package main

import (
	"flag"
	"fmt"
	//"log"
	"code.google.com/p/forestbus.server/server"
	"code.google.com/p/forestbus/rapi"
	"encoding/json"
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	"runtime"
	"strings"
	"time"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

const DEFAULT_PORT_NUMBER = ":3000"

const REQUIRED = "REQUIRED"

func main() {
	// Initialise our configuration from flags
	var clusterId = flag.String("id", "", "Cluster ID to apply the configuration to.")
	var nodes = flag.String("node", "", "Nodes to apply the configuration to.")

	flag.Usage = PrintUsage

	flag.Parse()

	// Get the commands
	nonFlagArgs := flag.Args()
	if len(nonFlagArgs) == 0 {
		fmt.Printf("No command given\n")
		PrintHelp()
		return
	}

	// if flag.Lookup("help") != nil || flag.Lookup("h") != nil {
	// 	PrintHelp()
	// 	return
	// }

	if len(*clusterId) == 0 && len(*nodes) == 0 {
		fmt.Printf("No cluster ID or nodes specified.\n")
		PrintHelp()
		return
	}

	var clusterNodes []string
	var err error

	if len(*clusterId) > 0 {
		clusterNodes, err = loadClusterConfig(*clusterId)
		if err != nil {
			fmt.Printf("Error reading nodelist for ID %v: %v\n", *clusterId, err)
			return
		}
	}

	nodesToConfigure := clusterNodes

	if len(*nodes) > 0 {
		nodesToConfigure = strings.Split(*nodes, ",")
	}

	var peers []string

	switch nonFlagArgs[0] {
	case "peers":
		if len(nonFlagArgs) < 2 {
			fmt.Printf("No peers specified.\n")
			PrintPeersHelp()
			return
		}
		peers = strings.Split(nonFlagArgs[1], ",")
		// If -id alone has been used, we need to send to all nodes in the current nodelist + new ones in the peerlist
		if len(*nodes) == 0 {
			// We must have used -id to get this far.
			// Merge lists
			nodesToConfigure = mergeNodeLists(clusterNodes, peers)
		}
		err = SendSetPeers(*clusterId, nodesToConfigure, peers)
		if err != nil {
			fmt.Printf("Error setting peers: %v\n", err)
		}
		if len(*nodes) == 0 {
			// We must have used -id to get this far.
			// Save the resulting nodelist file
			err = saveClusterConfig(*clusterId, peers)
			if err != nil {
				fmt.Printf("Error creating nodelist file: %v\n", err)
			}
		}
	case "topic":
		if len(nonFlagArgs) < 2 {
			fmt.Printf("No topic specified.\n")
			PrintTopicHelp()
			return
		}
		topicFlags := &flag.FlagSet{Usage: PrintTopicHelp}
		var segmentSize = topicFlags.Int("segmentSize", -1, "Target maximum segment size")
		var segmentCleanupAge = topicFlags.Int("segmentCleanupAge", -1, "Age in hours beyond which segments can be cleaned up.")
		topicFlags.Parse(nonFlagArgs[2:])

		err = SendTopic(*clusterId, nodesToConfigure, nonFlagArgs[1], *segmentSize, *segmentCleanupAge)
		if err != nil {
			fmt.Printf("Error handling topic request: %v\n", err)
		}
	default:
		fmt.Printf("Command %v not supported.\n", nonFlagArgs[0])
	}

	//fmt.Printf("Topic: %v, ClusterNodes: %v, nodesToConfigure: %v\n", topic, clusterNodes, nodesToConfigure)

	// newConfig := &server.ServerNodeConfiguration{}

	// if *peers != "" {
	// 	peerList := strings.Split(*peers, ",")
	// 	newConfig.Peers = append(newConfig.Peers, peerList...)
	// 	newConfig.Scope = server.CNF_Set_Peers
	// }

	// if *newTopic != "" {
	// 	newConfig.Topics = append(newConfig.Topics, *newTopic)
	// 	newConfig.Scope = server.CNF_Add_Topic
	// }

	// rpcclient, err := rpc.Dial("tcp", *nodeName)
	// if err != nil {
	// 	log.Fatal("dialing:", err)
	// }
	// reply := &server.ConfigChangeResults{}

	// log.Printf("Calling node to change configuration\n")
	// err = rpcclient.Call("RPCHandler.ConfigurationChange", newConfig, reply)
	// if err != nil {
	// 	log.Fatalf("Error getting messages from node: %v\n", err)
	// }
	// log.Printf("Got results: %v\n", reply.Success)
}

func loadClusterConfig(id string) (nodes []string, err error) {
	clusterFileName := id + ".nodelist"
	clusterFile, err := os.Open(clusterFileName)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	defer clusterFile.Close()

	data, err := ioutil.ReadAll(clusterFile)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &nodes)

	if err != nil {
		return nil, err
	}

	return nodes, err
}

func saveClusterConfig(id string, nodes []string) (err error) {
	clusterFileName := id + ".nodelist"
	clusterFile, err := os.Create(clusterFileName)
	if err != nil {
		return err
	}
	defer clusterFile.Close()

	data, err := json.Marshal(nodes)
	if err != nil {
		return err
	}
	clusterFile.Write(data)
	return nil
}

func sendConfiguration(nodesToConfigure []string, newConfig *server.ServerNodeConfiguration) (err error) {
	for _, node := range nodesToConfigure {
		netconn, localerr := net.Dial("tcp", node)
		//rpcclient, localerr := rpc.Dial("tcp", node)
		if localerr != nil {
			fmt.Printf("Error connecting to node %v: %v\n", node, localerr)
			err = localerr
		} else {
			rpcclient := rpc.NewClient(netconn)

			reply := &server.ConfigChangeResults{}

			localerr = rpcclient.Call("RPCHandler.ConfigurationChange", newConfig, reply)
			if localerr != nil {
				fmt.Printf("Error configuring node %v: %v\n", node, localerr)
				err = localerr
				return err
			}

			if reply.Result.Code != rapi.RI_SUCCESS {
				fmt.Printf("Error applying configuration to node %v: %v\n", node, reply.Result.Description)
			} else {
				fmt.Printf("Configuration set successfully on node %v\n", node)
			}
		}
	}

	return err
}

func SendSetPeers(clusterId string, nodesToConfigure []string, peers []string) (err error) {
	newConfig := server.GetEmptyConfiguration()

	newConfig.Peers = peers
	newConfig.Cluster_ID = clusterId
	newConfig.Scope = server.CNF_Set_Peers
	return sendConfiguration(nodesToConfigure, newConfig)
}

func SendTopic(clusterId string, nodesToConfigure []string, topicName string, segmentSize int, segmentCleanAgeHours int) error {
	newConfig := server.GetEmptyConfiguration()

	newConfig.Cluster_ID = clusterId
	newConfig.Scope = server.CNF_Set_Topic
	segmentCleanup := time.Duration(segmentCleanAgeHours) * time.Hour
	newConfig.Topics[topicName] = server.ConfigTopic{Name: topicName, SegmentSize: segmentSize, SegmentCleanupAge: segmentCleanup}
	if segmentCleanup >= 0 {
		fmt.Printf("Setting segment clean up age to: %v\n", segmentCleanup)
	}
	if segmentSize > 0 {
		fmt.Printf("Setting target segment size to %vMB\n", segmentSize)
	}
	return sendConfiguration(nodesToConfigure, newConfig)
}

func PrintUsage() {
	nonFlagArgs := flag.Args()
	if len(nonFlagArgs) == 0 {
		PrintHelp()
		return
	}

	switch nonFlagArgs[0] {
	case "peers":
		PrintPeersHelp()
	case "topic":
		PrintTopicHelp()
	default:
		PrintHelp()
	}
}

func PrintHelp() {
	fmt.Printf(`Forest-admin is a tool for managing running forest-bus clusters.
Usage: 

	forest-admin [-id <clusterID>] [-node node1[,...]] command [arguments]
When -id <clusterID> is specified without -node forest-admin reads the nodes to 
configure from a file “<clusterID>.nodelist” that is created with the peers 
command.

Adding -node sends commands to just the node(s) specified.

The commands are:

	peers		Sets the peer list for the given node or cluster ID.
	topic		Creates or updates the configuration of a given topic.

Use “forest-admin -help [command]” for more information about a command.

`)

}

func PrintPeersHelp() {
	fmt.Printf(`The forest-admin peers command sends the list of peers to nodes.
Usage:

     forest-admin -id <clusterID>] [-node node1[,...]] peers peer1[,...]

If a node is not listed in the peers, but was previously part of the nodelist 
saved under the cluster ID, the new peer list will be sent to that node so that 
it removes itself from the cluster.

If the -node flag is used then only the nodes listed will be sent the list of 
peers.  

If -id is used alone, then the list of nodes in the peer list is stored to the 
<clusterID>.nodelist file.  Any new nodes in the peer list are also sent the 
peer list.

`)

}

func PrintTopicHelp() {
	fmt.Printf(`The forest-admin topic command creates or updates topics.
Usage:

     forest-admin -id <clusterID>] [-node node1[,...]] topic <topicName> \
                 [-segmentSize=<target size in MB>] \
                 [-sgementCleanupAge=<age in hours>]

If the topic does not yet exist it is created.  If the topic already exists then
any parameters given are applied.

Parameters:

     -segmentSize        The target maximum size in MB that segments are allowed
                         to grow to before a new segment is added.

     -segmentCleanupAge  The age in hours after which segments can be cleaned
                         (deleted).  Set to 0 to disable clean-up.

`)

}

func mergeNodeLists(nodeLists ...[]string) []string {
	mergeMap := make(map[string]interface{})
	for _, nodeList := range nodeLists {
		for _, node := range nodeList {
			mergeMap[node] = struct{}{}
		}
	}
	mergedList := make([]string, 0, len(mergeMap))
	for k := range mergeMap {
		mergedList = append(mergedList, k)
	}
	return mergedList
}
