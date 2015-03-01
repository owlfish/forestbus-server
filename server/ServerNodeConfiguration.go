package server

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"
)

// ERR_INCORRECT_CLUSTER_ID is thrown if the configuration cluster ID doesn't match the expected one.
var ERR_INCORRECT_CLUSTER_ID = errors.New("Incorrect cluster ID")

// ERR_MISSING_CONFIG_NON_EMPTY_DIR is thrown if there are no configuration files present, but the directory given isn't empty.
var ERR_MISSING_CONFIG_NON_EMPTY_DIR = errors.New("Configuration files not found or could not be read, but the given directory is not empty and so no new configuration created.")

// The ConfigChangeType is used by the admin tool to identify what in a configuration request has changed.
type ConfigChangeType int64

var conf_log = log.New(os.Stderr, "Config: ", log.LstdFlags).Printf

const (
	// If CNF_Set_Peers is set then the configuration change includes a redfinition
	// of which Peers are available.
	CNF_Set_Peers ConfigChangeType = 1 << iota

	// If CNF_Set_Topic is set then the configuration change includes ensuring a topic exists.
	CNF_Set_Topic

	// If CNF_Remove_Topic is set then the configuration change lists the topics to be removed.
	CNF_Remove_Topic
)

// CONFIG_SUB_DIRECTORY is the name of the directory within the data path that holds the configuration files.
const CONFIG_SUB_DIRECTORY = "Config"

// CONFIG_FILENAME_CURRENT is the name of the currently active configuration file (stored in json)
const CONFIG_FILENAME_CURRENT = "config.cfg"

// CONFIG_FILENAME_OLD is the name of the previously active configuration file
const CONFIG_FILENAME_OLD = "config.old"

// CONFIG_FILENAME_NEW is the name of the new configuration file.  Once writing to config.new is complete, the config.cfg file is moved to config.old and then config.new is moved to config.cfg.
const CONFIG_FILENAME_NEW = "config.new"

// The DATA_SUB_DIRECTORY is the name of the data directory in the data path.  Each topic will be stored under a directory named after the topic in this directory.
const DATA_SUB_DIRECTORY = "Data"

/*
ConfigTopic holds the configuration information for a topic.
*/
type ConfigTopic struct {
	Name              string
	SegmentSize       int
	SegmentCleanupAge time.Duration
}

func (ct *ConfigTopic) String() string {
	return ct.Name
}

/*
GetDefaultTopicConfiguration returns the default configuration for a topic.
*/
func GetDefaultTopicConfiguration() ConfigTopic {
	conf := ConfigTopic{}
	conf.SegmentSize = 1024 // 1024 MB default segment size
	return conf
}

type ConfigTopics map[string]ConfigTopic

func (topics ConfigTopics) Contains(topicName string) bool {
	_, ok := topics[topicName]
	return ok
}

type ConfigPeers []string

func (peers ConfigPeers) Excluding(me string) ConfigPeers {
	newList := make(ConfigPeers, 0, len(peers))
	for _, peer := range peers {
		if peer != me {
			newList = append(newList, peer)
		}
	}
	return newList
}

func (peers ConfigPeers) Contains(peer string) bool {
	for _, p := range peers {
		if p == peer {
			return true
		}
	}
	return false
}

/*
The ServerNodeConfiguration structure holds the complete configuration for a cluster.  It is also used with bitmasks in the Scope field to communicate changes in configuration.
*/
type ServerNodeConfiguration struct {
	// Cluster_ID is used as a safety check to ensure that changes made and peers connecting are done against the correct cluster
	// Cluster_ID is recorded in the config file for a node and is passed by command line alone.
	Cluster_ID string
	// The Scope contains a bitmask of configuration changes represented by this data
	Scope ConfigChangeType
	// The list of Peers (name:port) that belong to this cluster
	Peers ConfigPeers
	// The map of Topics that belong to this cluster
	Topics ConfigTopics
	// List of paths data can be stored in - the last one is the default for new topics
	Data_Paths []string
	// Root path for this configuration (not persisted)
	root_path string
}

func GetEmptyConfiguration() *ServerNodeConfiguration {
	conf := &ServerNodeConfiguration{Topics: make(ConfigTopics)}
	return conf
}

/*
SaveConfiguration saves the configuration file as JSON in the conf.root_path

Logic:

	1 - Delete the config.new file if it already exists.
	2 - Create the new confing.new file.
	3 - Delete the config.old file if it already exists.
	4 - Move the existing confing.cfg file to config.old
	5 - Move the new config.new file to config.cfg
*/
func (conf *ServerNodeConfiguration) SaveConfiguration() error {
	oldFilename := path.Join(conf.root_path, CONFIG_SUB_DIRECTORY, CONFIG_FILENAME_OLD)
	currentFilename := path.Join(conf.root_path, CONFIG_SUB_DIRECTORY, CONFIG_FILENAME_CURRENT)
	newFilename := path.Join(conf.root_path, CONFIG_SUB_DIRECTORY, CONFIG_FILENAME_NEW)

	err := os.Remove(newFilename)
	if err != nil && !os.IsNotExist(err) {
		conf_log("Error cleaning up any old new save file: %v\n", err)
		return err
	}
	data, err := json.Marshal(conf)
	if err != nil {
		conf_log("Error marshalling configuration to json: %v\n", err)
		return err
	}

	file, err := os.Create(newFilename)
	if err != nil {
		conf_log("Error persisting config file: %v\n", err)
		return err
	}

	if _, err = file.Write(data); err != nil {
		conf_log("Error writing config data to file: %v\n", err)
		return err
	}

	if err = file.Close(); err != nil {
		conf_log("Error closing configuration file: %v\n", err)
		return err
	}

	err = os.Remove(oldFilename)
	if err != nil && !os.IsNotExist(err) {
		conf_log("Error cleaning up any old save file: %v\n", err)
		return err
	}

	err = os.Rename(currentFilename, oldFilename)
	if err != nil && !os.IsNotExist(err) {
		conf_log("Error moving current config file to old: %v\n", err)
		return err
	}

	if err = os.Rename(newFilename, currentFilename); err != nil {
		conf_log("Error moving new configuration file into place: %v\n", err)
		return err
	}
	return nil
}

/*
LoadConfiguration is responsible for determining what server configuration is present at the given path.

The logic followed is:

	1 - Is the rootpath a directory?
	2 - Is the directory empty - if so create a skeleton configuration
	3 - If the directory has an entry called "Config" look inside it
	4 - If a config.cfg file exists, read it
	5 - If a config.cfg file is not present, but a config.old is present, move it to config.cfg
	6 - If the configuation cluster_ID is empty, set it to the clusterID
	7 - If the configuration cluster_ID is not empty and doesn't match clusterID - error.
*/
func LoadConfiguration(clusterID, rootpath string) (*ServerNodeConfiguration, error) {
	dirContents, err := ioutil.ReadDir(rootpath)
	if err != nil {
		conf_log("Error reading directory path contents of %v: %v\n", rootpath, err)
		return nil, err
	}
	// If the directory is empty, go and create new configuration.
	if len(dirContents) == 0 {
		return createNewConfiguration(clusterID, rootpath)
	}

	foundConfig := false
	for _, dirEntry := range dirContents {
		if dirEntry.Name() == CONFIG_SUB_DIRECTORY && dirEntry.IsDir() {
			foundConfig = true
			break
		}
	}
	// If there is no configuration sub directory, but there is something there - return an error
	if !foundConfig {
		conf_log("No configuration directory found in %v, but directory is not empty so unable to create a new configuration\n", rootpath)
		return nil, ERR_MISSING_CONFIG_NON_EMPTY_DIR
	}

	currentConfigFileName := path.Join(rootpath, CONFIG_SUB_DIRECTORY, CONFIG_FILENAME_CURRENT)
	oldConfigFileName := path.Join(rootpath, CONFIG_SUB_DIRECTORY, CONFIG_FILENAME_CURRENT)

	_, err = os.Stat(currentConfigFileName)
	if err != nil {
		if os.IsNotExist(err) {
			// Configuration file not there - try looking for an old one.
			_, err = os.Stat(oldConfigFileName)
			if err != nil {
				if os.IsNotExist(err) {
					// No config file found.
					conf_log("No configuration file or old configuration file found in %v\n", path.Join(rootpath, CONFIG_SUB_DIRECTORY))
					return nil, ERR_MISSING_CONFIG_NON_EMPTY_DIR
				}
				conf_log("Unable to read configuration file or old configuration file in %v\n", path.Join(rootpath, CONFIG_SUB_DIRECTORY))
				return nil, ERR_MISSING_CONFIG_NON_EMPTY_DIR
			}

			// We have an old configuration file - try and move it back into place.
			err = os.Rename(oldConfigFileName, currentConfigFileName)
			if err != nil {
				conf_log("Unable to rename old configuration file to current configuration file in %v: %v\n", path.Join(rootpath, CONFIG_SUB_DIRECTORY))
				return nil, err
			}
			// If we make it this far then currentConfigFileName should exists.
			_, err = os.Stat(currentConfigFileName)
			if err != nil {
				conf_log("Unable to read old recovered configuration file %v\n", currentConfigFileName)
				return nil, err
			}
		}
		conf_log("Unable to read configuration file %v: %v\n", currentConfigFileName, err)
		return nil, err
	}

	// We have a configuration file!
	jsondata, err := ioutil.ReadFile(currentConfigFileName)
	if err != nil {
		conf_log("Error reading configuration file %v: %v\n", currentConfigFileName, err)
		return nil, err
	}

	config := GetEmptyConfiguration()
	if err = json.Unmarshal(jsondata, config); err != nil {
		conf_log("Error parsing json configuration file %v: %v\n", currentConfigFileName, err)
		return nil, err
	}

	if config.Topics == nil {
		// This shouldn't happen, but if it does, recover from it.
		conf_log("WARNING: Topics configuration data missing\n")
		config.Topics = make(ConfigTopics)
	}

	if len(config.Cluster_ID) > 0 && config.Cluster_ID != clusterID {
		conf_log("Cluster ID %v in configuration file doesn't match given cluster ID of %v\n", config.Cluster_ID, clusterID)
		return nil, ERR_INCORRECT_CLUSTER_ID
	}

	config.root_path = rootpath

	if len(config.Cluster_ID) == 0 && len(clusterID) > 0 {
		config.Cluster_ID = clusterID
		err = config.SaveConfiguration()
		if err != nil {
			conf_log("Error updating cluster ID and saving configuration: %v\n", err)
			return nil, err
		}
	}

	return config, nil
}

/*
createNewConfiguration creates a default configuration file and saves it.

Logic:

	1 - Create the Config directory
	2 - Create a new ServerNodeConfiguration object
	3 - Save it
*/
func createNewConfiguration(clusterID, rootpath string) (*ServerNodeConfiguration, error) {
	configDir := path.Join(rootpath, CONFIG_SUB_DIRECTORY)
	if err := os.Mkdir(configDir, os.ModePerm); err != nil {
		conf_log("Error creating configuration sub-directory: %v\n", err)
		return nil, err
	}
	config := GetEmptyConfiguration()
	config.Cluster_ID = clusterID
	config.Data_Paths = append(config.Data_Paths, path.Join(rootpath, DATA_SUB_DIRECTORY))
	err := os.Mkdir(path.Join(rootpath, DATA_SUB_DIRECTORY), os.ModePerm)
	if err != nil {
		conf_log("Error creating data sub directory %v: %v\n", path.Join(rootpath, DATA_SUB_DIRECTORY), err)
		return nil, err
	}
	config.root_path = rootpath
	if err := config.SaveConfiguration(); err != nil {
		conf_log("Error saving configuration file in createNewConfiguration: %v\n", err)
		return nil, err
	}
	return config, nil
}
