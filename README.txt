Forest Bus (Version 1.0)
------------------------
Forest Bus is a message bus based on a persistent distributed commit log, very 
much inspired by Apache Kafka.  Forest Bus is written in Go with client 
libraries available for Go, Java and Python.

A distributed commit log message bus allows clients to request any message 
within a topic.  This allows clients to bootstrap over historical data, 
reprocess messages following software fixes or enhancements and catch-up 
following outages.  Forest Bus allows clients to read from any node in a 
cluster, helping to distribute the load of serving messages.

Installation (binary distribution)
----------------------------------
The Forest Bus commands in the bin directory are standalone executables 
with no external dependencies.  As such they can be copied to anywhere 
in your path (e.g. /usr/local/bin under Linux).

The binaries included are:

forest-bus-server - The server binary for Forest Bus
forest-admin      - Configuration administration tool
forest-send       - Tool for sending messages from stdin to a Forest Bus topic
forest-get        - Tool for getting messages from Forest Bus topic to stdin
forest-test-send  - Benchmarking tool for simulating multiple clients

Installation (source distribution)
----------------------------------
Forest Bus is written in Go (http://golang.org/), so needs Go to be installed 
in order to be built from source.

The source code is distributed under src and your GOPATH should be set to the 
top of the Forest Bus directory.

The Forest Bus depenencies can then be installed (Mercurial and Git clients 
will be required):

go get code.google.com/p/forestbus
go get github.com/ugorji/go/codec

Each binary can then be built in turn:

go install code.google.com/p/forestbus.server/cmd/forest-admin
go install code.google.com/p/forestbus.server/cmd/forest-bus-server
go install code.google.com/p/forestbus.server/cmd/forest-get
go install code.google.com/p/forestbus.server/cmd/forest-send
go install code.google.com/p/forestbus.server/cmd/forest-test-send
  
Notes
-----
This code is made freely available under an MIT license, see LICENSE.txt 
for more details.

Documentation
-------------
Please refer to http://owlfish.com/software/ForestBus/ for documentation on 
how to use Forest Bus.
