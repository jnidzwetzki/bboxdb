# Changelog

### Version 0.2.0 (Beta-3) - TBA
- New Feature: Introduced paging
- Improvement: Simplified network protocol by removing the duplicate success and response packages
- Improvement: Success and Error responses are now indicated by the isFailed() future method 
- Improvement: Success and Error messages are now indicated by the getMessage() future method 
- Bugfix: Group membership watch was not re-established
- Bugfix: Wrong node state was displayed in the GUI

### Version 0.2.0 (Beta-2) - 28.11.2016
- New Feature: Java 8 is now required to build the project
- New Feature: Delete tuple operations now contain a timestamp to order them correctly
- New Feature: Introduced a bloom filter for memtables and sstables
- Improvement: Added missing copyright header to all source files
- Improvement: OSM import is now executed on a disk backed set, so huge imports (> memory) can be performed
- Improvement: Speed up 'read by key' operation, by scanning only the relevant SStables
- Improvement: Switched to SLF4J LOG4J 12 Binding 1.7.21 and Zookeeper 3.4.9
- Improvement: The server don't materialize the query results. This allows huge query results (> memory)
- Bugfix: Logs now written into $installdir/logs
- Bugfix: Outdated tuples could be returned from the unflushed memtables
- Bugfix: Fixed 'node exist exception' during instance registration on fast service restarts
- Bugfix: Remove outdated jars on upgrade from classpath
- Bugfix: Not all relevant tuples are scanned by the 'read by key' operation
- Bugfix: Set correct (millisecond based) timestamps for tuples on creation

### Version 0.2.0 (Beta-1) - 17.11.2016
- New feature: Added a selftest
- Improvement: Implemented keep alive packages, to keep tcp connections open
- Improvement: Added the possibility to log debug messages 
- Improvement: Write geometrical data to server in OSMInsertBenchmark
- Improvement: Moved OSM importer code to own class
- Bugfix: Removed duplicate logging entries
- Bugfix: Removed deadlock on node down event
- Bugfix: Skip compact operation when only one SSTable exists
- Bugfix: Set failed state on future when server operation returns an error
- Bugfix: Flush full memtables also on tuple delete operation
- Bugfix: Fixed some slf4j logging issues
- Bugfix: Result set was containing outdated tuples

### Version 0.1.2 (Alpha) - 14.09.2016
- Implemented the recovery service
- Unified the structure of request and response packages
- Introduced connection capabilities and connection handshaking
- Implemented network compression
- Improved network package decoding
- Introduced the WeightBasedSplitStrategy

### Version 0.1.1 (Alpha) - 26.08.2016
- Introduced replication strategies
- Handle pending futures in benchmarks correctly and limit the number of pending requests
- Implemented roads in OSM Benchmark
- Improved states for DistributionRegions
- Implemented checkpoints
- Store node states in zookeeper

### Version 0.1.0 (Alpha) - 07.08.2016
- Fixed some crashes in the network handler
- Added first version of the OSM data import benchmark
- Added routing header to network packages 
- First version with working ClusterClient
- Changed client API (Introduced multi result futures)
- Implemented insert request routing
- Changed the structure of the list tables response package
- Spread existing data on region split

### Version 0.0.9 (Alpha) - 14.07.2016
- Added compactification statistics
- Introduced a simple split strategy
- Store the version number of the instances in zookeeper
- Data is now stored in region tables
- Added version number to distribution groups
- Added state field to distribution groups
- Implemented membership connection service

### Version 0.0.8 (Alpha) - 22.06.2016
- Store Distribution Region assignment in zookeeper
- Improved exception handling and prevent half written sstables
- Introduced the multi server client "ScalephantCluster"
- Made GUI settings (ZookeeperHost, Clustername) configurable
- The GUI now uses data fetched from zookeeper instead of mockup data
- The nameprefix of the distribution regions is now stored in zookeeper
- Introduced a simple resource allocation strategy

### Version 0.0.7 (Alpha) - 05.06.2016
- Improved bounding box implementation
- Improved distribution group GUI handling
- Added create and delete distribution group network packages
- Replication factor is now configurable per distribution group
- Added logic to store distribution groups in zookeeper
- Added a in memory structure for distribution groups (updated by zookeeper) 
- Added zookeeper to travis ci environment
- Added zookeeper integration tests

### Version 0.0.6 (Alpha) - 13.05.2016
- Added timestamp queries
- Implemented the table transfer network package
- Changed the requestid of the network protocol to int, to handle more parallel requests
- Removed a lot of buffers in the network implementation (less memory is needed)
- Implemented distributed instance discovery via zookeeper 
- Added a basic GUI
- The zookeeper database can be now deleted with the cluster management script

### Version 0.0.5 (Alpha) - 24.03.2016
- Added basic benchmarks
- Added a logo
- Improved compaction strategy
- Introduced major compactions
- Improved SSTable usage counting
- Added SStable metadata
- Added distributed membership management
- Added a cluster management script
- Integrated zookeeper

### Version 0.0.4 (Alpha) - 03.03.2016
- Added one client example 
- Added configuration file (scalephant.yaml)
- Added support for multiple tuple result queries
- Implemented BoundingBoxes
- Implemented a naming scheme for tables
- Added a start and stop script for Linux (using Apache jsvc)

### Version 0.0.3 (Alpha) - 16.02.2016
- Implemented a network client
- First network protocol specification (see doc/network.md)
- Implemented a server service
- Integrated travis-ci

### Version 0.0.2 (Alpha) - 26.01.2016
- Implemented a SSTable/SSTableIndex examiner for debugging
- Introduced a simple compactification strategy
- Introduced SSTable indices  
- Implemented index binary search, to locate tuples
- Implemented a tuple iterator to perform full table scans
- Handle deleted tuples correctly 
- Implemented SSTable compactification
- Added MultiThreadling support in the SSTableManager
- Switched the reader from File IO to Memory Mapped IO

### Version 0.0.1 (Alpha) - 20.11.2015
- Initial release
