---
layout: page
title: "Changelog"
category: dev
date: 2016-12-12 22:46:12
order: 1
---
### Version 0.2.5 (Stable) - TBA

### Version 0.2.4 (Stable) - 23.04.2017
- New Feature: Support multiple storage locations
- New Feature: Added maven packaging
- New Feature: Added a received timestamp to tuples
- New Feature: Introduced the OSM converter
- Improvement: Experiments are now executed on a preprocessed OSM dataset
- Improvement: Switched OSM converter from MapDB to h2 and Berkeley DB (Java Edition) backend
- Improvement: Added a OSM filter for woods
- Improvement: Improved thread stop handling
- Improvement: Introduced the memtable flush callbacks
- Improvement: Improved .jar discovery in bash files
- Bugfix: SStables were written unbuffered. This requires a lot of CPU time and was slow.
- Bugfix: Limit the size of SSTables to 1.9 GB. Otherwise, the reader is unable to map the table into memory.
- Bugfix: Limit the amount of maximal unflushed memtables. (Prevents OOM Exceptions on heavy write load)
- Bugfix: Removed the unused 'commit log' variable from config class
- Bugfix: Removed the unused 'seen' state from tuple class
- Bugfix: Removed warning caused by rm on first bboxdb_update call
- Bugfix: Removed unused jsvc_* variables from bash scripts

### Version 0.2.3 (Stable) - 09.04.2017
- New Feature: Futures now store the completion time 
- New Feature: Introduced the tuple deletion without timestamp method in API
- New Feature: Changed the format of the compressed envelopes to support multi-package compression
- New Feature: Added parser for simple GEOJson data
- New Feature: Introduced client and server side batch compression
- Improvement: Enabled paging as default setting (50 tuples per page)
- Improvement: Switched tuple timestamps from milliseconds to microseconds
- Improvement: Added water OSM entity filter
- Improvement: Speed up the OSM importer by using a new node serializer 
- Improvement: Updated to org.yaml 1.18 (from 1.17) / Mockito 2.7.19 (from 2.7.5)
- Improvement: Refactored the server side network package handling
- Improvement: Refactored the client side network package handling
- Improvement: Changed the server side connection error handling
- Improvement: Data is automatically placed into the "data" subdirectory
- Improvement: Default data location is now /tmp/bboxdb
- Bugfix: Removed the unused 'root directory' from config file

### Version 0.2.2 (Stable) - 23.03.2017
- New Feature: Added start option for remote debuging
- New Feature: Added OSM viewer for K-D Trees
- New Feature: Added sampling size experiment 
- Improvement: Moved scripts from misc/ to bin/ folder
- Improvement: New file system layout (all tables of a distribution group are now located in one dir)
- Improvement: In memory data is also redistributed after a region split
- Improvement: The version for every distribution group is stored locally
- Improvement: The local version and the remote version of the distribution group is checked. This prevents node joins with outdated data
- Improvement: One part of the data is stored locally after region split
- Bugfix: A synchronisation issue in the memtable flush thread is fixed
- Bugfix: The connect dialog in the GUI is now big enough to show full IP-Addresses
- Bugfix: Empty sstable dirs are removed after region split
- Bugfix: Region mapping is removed after region split
- Bugfix: Region mapping is not created for inactive regions

### Version 0.2.1 (Stable) - 31.01.2017
- New Feature: Using Coverity scan and codecov.io to improve source code quality
- New Feature: OSM Data is now stored as GeoJSON, instead of a java serialized byte stream
- New Feature: Introduced the R-Tree spatial indexer
- New Feature: Added index support to the query processor
- New Feature: Introduced query plans
- New Feature: Implemented timeout for futures
- Improvement: Changed bounding box precision from float to double
- Improvement: Reduced memory usage of the network stack by removing the response buffering
- Improvement: Switched from MapDB 3.0.2 to 3.0.3, and from guava 20.0 to 21.0
- Improvement: Fixed a lot of small bugs, found by 'Coverity code' scan
- Improvement: Region splits are now based on the size of the region
- Improvement: SSTables and SSTable-Index files are using now different magic bytes
- Improvement: The timestamp query only reads tuple stores which contain tuples for this timestamp
- Improvement: Added timeout to routing requests
- Bugfix: Outdated jars are new removed on bboxdb_update
- Bugfix: Removed implementation and specification of the unused TransferSSTable network package
- Bugfix: Removed the non working BoxSearch spatial index
- Bugfix: The weight based split strategy was analyzing all local regions of a distribution group

### Version 0.2.0 (Stable) - 11.01.2017
- Improvement: New logo
- Improvement: Shortened socket close exception
- Improvement: Write statistical data about flushed memtables
- Improvement: Added callbacks for distribution region changes
- Improvement: Reduced GUI CPU usage
- Improvement: The GUI now shows a waiting cursor, when the distribution group is loaded
- Improvement: The component size of the main panel now consider only the shown components
- Improvement: Introduced a status bar in the GUI main view
- Improvement: Switched GUI to Nimbus look and feel
- Improvement: The region id of a distribution group is now printed in the GUI
- Bugfix: The wrong split interval was taken
- Bugfix: Fixed a race condition on compact thread init
- Bugfix: The max size of a memtable was treated as kilobytes, not bytes
- Bugfix: Ensure that the data of the parent region is redistributed completely until child region is splitted
- Bugfix: The same instance of the RegionSplitStrategy was used for all Tables, which leads to wrong data redistribution
- Bugfix: Ensure that the RessourcePlacementStrategyFactory create new instances
- Bugfix: Removed repeated log messages about adding local region mappings
- Bugfix: All local tables (not only the tables of a certain region) have been redistributed
- Bugfix: Removed warning when merging an empty bounding box
- Bugfix: Fixed handling of empty tooltips in the GUI
- Bugfix: Non-overlapping placement of distribution groups in the GUI
- Bugfix: New distribution regions in zookeeper was set to READY instead of CREATING before system where added
- Bugfix: Fixed calculation of the root node pos in the GUI

### Version 0.2.0 (Beta-5) - 01.01.2017
- Improvement: Introduced JVM parameter
- Improvement: Enabled the JMX server
- Improvement: Replaced 'apache commons daemon' BBoxDB process management with native Java solution
- Improvement: Assertions are now enabled during unit tests and in server mode
- Improvement: Introduced distribution group selection in GUI
- Improvement: Added trace start to bboxdb
- Improvement: Added the ability to reread distribution group list in GUI
- Improvement: Unflushed memtables are now flushed on shutdown
- Improvement: Improved exception handling during server start/stop
- Improvement: SSTables can be deleted without init the sstable manager
- Improvement: Log statistics about redistributed tuples
- Bugfix: Allocate systems to new distribution regions, before then come ready
- Bugfix: GUI is repainted when a distribution region is recreated
- Bugfix: Size of the GUI tree component was not dynamically calculated 
- Bugfix: The acquire storages code releases all (not only the already acquired) storages on failure, this leads to wrong usage counting
- Bugfix: Regions couldn't be removed from the name prefix manager
- Bugfix: No data is written to ACTIVE_FULL regions
- Bugfix: For splits, only the part of the tuple is analyzed, that is covered by the region
- Bugfix: State updates of the tree are applied after reading the child nodes. Otherwise, a region could be set to splitted before the child nodes are ready 
- Bugfix: Nodes in state 'creating' are not considered as real child nodes in isLeafNode() method

### Version 0.2.0 (Beta-4) - 20.12.2016
- New Feature: Renamed project from 'scalephant' to 'BBoxDB'
- New Feature: Introduced the new $BBOXDB_HOME environment variable
- New Feature: Added cluster debug startup to manage_cluster.sh script 
- Improvement: Unified tuple store aquire code
- Improvement: Introduced the local selftest
- Improvement: Added CLI parameter to the SSTableExaminer
- Improvement: The documentation is now pushed on 'GitHub pages'
- Improvement: Rewrote the KD-Tree zookeeper integration code
- Improvement: Improved handling of deleted distribution groups
- Improvement: Node configuration is now performed via files (not via environment variables)
- Improvement: Ensure that only one node splits a distribution region at the same time (new state ACTIVE_FULL)
- Improvement: Introduced server error messages
- Bugfix: Init table splitter only in distributed mode
- Bugfix: In a major compact, not all tables were processed
- Bugfix: After distribution group delete / create the DistributionGroupCache can become inconsistent

### Version 0.2.0 (Beta-3) - 09.12.2016
- New Feature: Introduced paging (results > memory could be requested)
- New Feature: Introduced type safe client API
- New Feature: Added boundingbox with time query type
- New Feature: Added the cancel query server call
- Improvement: Simplified network protocol by removing the duplicate success and response packages
- Improvement: Success and Error responses are now indicated by the isFailed() future method 
- Improvement: Success and Error messages are now provided by the getMessage() future method 
- Improvement: Introduced iterator for list results in client API
- Improvement: Use the WeightBasedSplitStrategy as default
- Improvement: Clean unused memtables to support the GC
- Bugfix: Group membership watch was not re-established
- Bugfix: Wrong node state was displayed in the GUI
- Bugfix: Fixed handling of failed futures in the benchmark
- Bugfix: Fixed calculation of 'in-flight calls' for a cluster
- Bugfix: Fixed calculation of the routing header, don't route packages to local instance
- Bugfix: Ignore socket exception, when a server shutdown is performed
- Bugfix: When a dead instance is detected, don't send a disconnect package. Close the network connection instead
- Bugfix: Fixed division by zero in WeightBasedSplitStrategy
- Bugfix: Thread names contain the full output of sstablename.toString
- Bugfix: The checkpoint date is not written to zookeper, if no memtable was flushed
- Bugfix: The tuple redistribution strategy was not able to handle local destinations 
- Bugfix: Fixed NPE during bounding box creation
- Bugfix: Deleted tuples were read form SSTables without the original timestamp
- Bugfix: Duplicate tuples (caused by queries on replicates) are filtered in the client code

### Version 0.2.0 (Beta-2) - 28.11.2016
- New Feature: Java 8 is now required to build the project
- New Feature: Delete tuple operations now contain a timestamp to order them correctly
- New Feature: Introduced a bloom filter for memtables and sstables
- Improvement: Added missing copyright header to all source files
- Improvement: OSM import is now executed on a disk backed set, so huge imports (> memory) can be performed
- Improvement: Speed up 'read by key' operation, by scanning only the relevant SStables
- Improvement: Switched to SLF4J LOG4J 12 Binding 1.7.21 and Zookeeper 3.4.9
- Improvement: The server does not materialize the query results. This allows huge query results (> memory)
- Bugfix: Logs now written into $installdir/logs
- Bugfix: Outdated tuples could be returned from the unflushed memtables
- Bugfix: Fixed 'node exist exception' during instance registration on fast service restarts
- Bugfix: Remove outdated jars on upgrade from classpath
- Bugfix: Not all relevant tuples are scanned by the 'read by key' operation
- Bugfix: Set correct (millisecond based) timestamps for tuples on creation

### Version 0.2.0 (Beta-1) - 17.11.2016
- New feature: Added a selftest
- Improvement: Implemented keep alive packages, to keep TCP connections open
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
- Changed client API (Introduced multi-result futures)
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
- Introduced the multi-server client "ScalephantCluster"
- Made GUI settings (ZookeeperHost, Clustername) configurable
- The GUI now uses data fetched from zookeeper instead of mockup data
- The name prefix of the distribution regions is now stored in zookeeper
- Introduced a simple resource allocation strategy

### Version 0.0.7 (Alpha) - 05.06.2016
- Improved bounding box implementation
- Improved distribution group GUI handling
- Added create and delete distribution group network packages
- Replication factor is now configurable per distribution group
- Added logic to store distribution groups in zookeeper
- Added an in-memory structure for distribution groups (updated by zookeeper) 
- Added zookeeper to Travis CI environment
- Added zookeeper integration tests

### Version 0.0.6 (Alpha) - 13.05.2016
- Added timestamp queries
- Implemented the table transfer network package
- Changed the requestid of the network protocol to int, to handle more parallel requests
- Removed many buffers in the network implementation (less memory is needed)
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
- Integrated Travis CI

### Version 0.0.2 (Alpha) - 26.01.2016
- Implemented a SSTable/SSTableIndex examiner for debugging
- Introduced a simple compactification strategy
- Introduced SSTable indices  
- Implemented binary index search, to locate tuples
- Implemented a tuple iterator to perform full table scans
- Handle deleted tuples correctly 
- Implemented SSTable compactification
- Added MultiThreadling support in the SSTableManager
- Switched the reader from File IO to Memory Mapped IO

### Version 0.0.1 (Alpha) - 20.11.2015
- Initial release
