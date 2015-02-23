/*
utils contains common code used through-out the Forest Bus server implementation.
*/
package utils

import (
	"fmt"
	"log"
	"os"
)

// PrintFFunc is a type definition for the log.Printf function used as a field in a number of objects that require logging services.
type PrintFFunc func(format string, args ...interface{})

// GetTopicLogger returns a PrintFFunc that prefixes messages with the topic and component name.
func GetTopicLogger(topicName, component string) PrintFFunc {
	return log.New(os.Stderr, fmt.Sprintf("Topic %v (%v): ", topicName, component), log.LstdFlags).Printf
}

// GetServerLogger returns a PrintFFunc that prefixes messages with a component name.
func GetServerLogger(component string) PrintFFunc {
	return log.New(os.Stderr, fmt.Sprintf("%v: ", component), log.LstdFlags).Printf
}
