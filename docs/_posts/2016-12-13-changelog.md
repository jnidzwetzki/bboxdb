---
layout: page
title: "Changelog"
category: dev
date: 2016-12-12 22:46:12
order: 1
---

### Version 0.5.0 (Stable) - TBA
- Improvement: Moved region id mapper into the space partitioner to ensure data integrity
- Bugfix: Wait before data is merged / distributed until we see the region change in zookeeper (closes #75)
- Bugfix: The average region is size is used in merges to prevent merges directly after split (closes #76)
- Bugfix: The JMX space partitioner call has become useless after region merging was introduced, dropped support 

### Version 0.4.2 (Stable) - 21.02.2018
- New Feature: Added data loader for re-balance demonstration
- New Feature: Added gossip for eventual consistency
- New Feature: Added read repair for eventual consistency
- Improvement: Updated dependencies (json / guava)
- Improvement: Rewrote shutdown code 
- Improvement: If the server is still running 60 seconds after a shutdown, it is killed
- Improvement: Check for enough split points on sampling
- Improvement: Routed packages with empty tuplestore list are not send to server
- Improvement: All network requests are resubmitted on failure
- Improvement: Added some delay before failed network requests are resubmitted
- Improvement: Made Zookeeper configuration simpler
- Improvement: Region statistics are now written by a dedicated thread
- Improvement: Updated screenshots
- Improvement: Node stats are updated periodically
- Improvement: On a region split, one region is allocated to another system
- Improvement: When all tables are included in a minor compact, handle it as major compact
- Improvement: Added full dump option to the SSTable examiner
- Improvement: Change instance state from 'unknown' to 'failed'
- Bugfix: Fixed dimensions / distribution group name in OSM SSTable converter
- Bugfix: Prevent waiting on already closed network connections
- Bugfix: Zookeeper initial population can be done in parallel
- Bugfix: Network sockets are closed, when a IOException has occurred
- Bugfix: Package was not routed to the last hop of routing list
- Bugfix: Ensure distribution group is read when local mappings are requested
- Bugfix: Duplicates are removed from the simple iterator result set
- Bugfix: Fixed connection / complete result state in futures on merge operation
- Bugfix: Fixed table creation in parallel in InsertTupleHandler
- Bugfix: Handle deleted tuples in SamplingBasedSplitStrategy properly
- Bugfix: Fixed several NPEs in RegionSplitter
- Bugfix: Fixed Zookeeper tuplestore path determination with prefix
- Bugfix: Local tuple store configuration is written in tuple store split
- Bugfix: Deleted tuples are also redistributed
- Bugfix: Don't include null memtable in getAllTupleStorages to prevent NPEs
- Bugfix: The routing list is recalculated on package re-submission
- Bugfix: Track sequence number usage and prevent duplicate use
- Bugfix: Fixed handling of null tuples in SpatialIndexReader
- Bugfix: Fixed detection of the Zookeeper client in GUI
- Bugfix: System path /tables are not reported as distribution group
- Bugfix: Region statistics can now be updated
- Bugfix: Merge test is only executed if we are responsible for the parent region
- Bugfix: Old statistics are cleared after region split is done
- Bugfix: Old tuples are deleted in major compaction
- Bugfix: Merging locally stored data was not supported
- Bugfix: Mapped distribution regions are removed after merge
- Bugfix: Fixed deadlock between region id mapper and space partitioner cache
- Bugfix: Deleted distribution groups are now removed from the KD-Tree in memory version

### Version 0.4.1 (Stable) - 02.02.2018
- New Feature: Implemented the spatial join operation
- New Feature: Added Joinable Tuple to the network protocol
- Improvement: Re-implemented server side query processor
- Improvement: Updated prometheus dependency
- Improvement: Changed the naming of the distribution groups/tables to dgroup_table
- Improvement: Introduced the distribution group config cache
- Bugfix: Don't include non overlapping tuple versions in the bounding box query
- Bugfix: Fixed NPE in closeable helper
- Bugfix: Fixed a bug in the interval overlapping calculation
- Bugfix: Handle zookeeper connect failed
- Bugfix: Fixed forced connection shutdown timeout
- Bugfix: Ensure that the microsecond timestamp provider does not return duplicate timestamps

### Version 0.4.0 (Stable) - 21.01.2018
- New Feature: Size in byte and tuples are stored in Zookeeper
- New Feature: Added distribution region merge feature
- Improvement: Introduced the generic space partitioner interface
- Improvement: Created bboxdb_tools module
- Improvement: Clean up dependencies of the modules
- Improvement: Moved experiments to a new subproject
- Improvement: Size and tuples per distribution region are shown in the GUI
- Improvement: Changed distribution region id form int to long
- Improvement: Added JMX call to redistribute regions

### Version 0.3.7 (Stable) - 14.12.2017
- New Feature: Distribution regions now have a minimal size
- Improvement: Added usage counter to Zookeeper connection to prevent shutdown during event handler runs
- Improvement: Modularize maven project

### Version 0.3.6 (Stable) - 20.11.2017
- New Feature: Added continuous bounding box query
- New Feature: Added performance counter / instrumentation (implemented with prometheus) 
- Improvement: Made memory statistics logging configurable via const
- Improvement: Added insert callback to TupleStore
- Improvement: The spatial index now contains the tuple byte positions
- Improvement: Switched from Zookeeper 3.4.10 to Zookeeper 3.4.11
- Improvement: Improved client connection error handling
- Bugfix: Paging results can now contain equal keys
- Bugfix: Improved exception handling on network header encoding
- Bugfix: Fixed some bugs found by coverty scan
- Bugfix: Prevent duplicate MBean register exception
- Bugfix: Fixed a bug in the string to boolean converter
- Bugfix: Fixed the service callback unregister method

### Version 0.3.5 (Stable) - 06.11.2017
- New Feature: Made project compatible with sonatype.org hosting
- New Feature: This project is now available in the 'Maven Central Repository'
- New Feature: Added an example how to work with duplicate keys and the tuple history
- New Feature: Added client software section in documentation
- Improvement: Updated dependencies (SnakeYAML, osmosis, org.json, Mockito)

### Version 0.3.4 (Stable) - 03.11.2017
- New Feature: Added a experiment to determine the bloom filter efficiency
- New Feature: Tuple Storages now have a configuration
- New Feature: Implemented duplicate key tuple stores
- New Feature: Implemented TTL tuple stores
- New Feature: get(key) can now return multiple tuples
- New Feature: Added an update anomaly resolver
- New Feature: Added the possibility to transfer deleted tuples as a result (needed for recovery and tuple stores that allow duplicates)
- Improvement: Added a glossary and renamed a lot of classes
- Improvement: Added logging parameter to CLI
- Improvement: Shortened BBox output in CLI
- Bugfix: The CLI don't unregister the local node in Zookeeper

### Version 0.3.3 (Stable) - 07.09.2017
- New Feature: Added routing header to delete and query packages
- New Feature: Introduced the key index cache
- New Feature: Added the create table call
- New Feature: Introduced the SSTable configuration
- Improvement: Removed a lot of parameter from the create distribution group BBoxDB call
- Improvement: Switched from Guava 22 to 23
- Improvement: Added create and delete table call to CLI
- Improvement: Added the SSTable key cache experiment
- Improvement: Removed split strategy from configuration file and made it space partitioned depended
- Bugfix: Fixed a race condition in the tuple list store
- Bugfix: Fixed a bug in the query type field location

### Version 0.3.2 (Stable) - 10.08.2017
- New Feature: The CLI shows all discovered BBoxDB instances
- New Feature: Introduced client based insert tuple routing
- New Feature: Write Hardware info (cpu cores, memory, diskspace) to Zookeeper
- New Feature: Added cpu core, storages, free diskspace and memory placement strategies
- New Feature: Region size, placement strategy and space partitioner are specified per distribution group
- New Feature: Introduced placement strategy and space partitioner configuration
- New Feature: Added the TupleListFutureStore
- Improvement: Added importer for TPC-H order datasets
- Improvement: Implement insert operation retry on error
- Bugfix: Fixed wait for pending calls method in client code

### Version 0.3.1 (Stable) - 29.06.2017
- Improvement: A fixed amount of memtable flush threads is used per storage
- Improvement: Only one checkpoint thread per storage
- Improvement: Only one compact thread per storage 
- Improvement: Only one split task per storage 
- Improvement: Made service init interruptable
- Improvement: Better spread statistics
- Improvement: Enabled Zookeeper logging
- Improvement: Removed StorageRegistry singleton
- Improvement: Added unmapper for memory mapped regions
- Improvement: The R-Tree index is now written to disk. In previous versions, the index was recalculated on every SSTable opening
- Improvement: The R-Tree index is now calculated non-recursive to safe call stack memory
- Improvement: The R-Tree index now is based on tuple positions instead of keys
- Improvement: Made the Spatial index reader configurable
- Improvement: Check for 64 bit environment on start
- Improvement: Limit the amount of parallel queries per client
- Improvement: Client flush queue on next page request
- Improvement: Introduced the maximal unflushed package queue size
- Bugfix: Prevent duplicate distribution of in-memory data
- Bugfix: Handle non empty systems list in tuple insert as error
- Bugfix: Reread systems list on insert tuple, when no systems are detected for BBox
- Bugfix: Fixed handling of meta data when multiple storage locations are used
- Bugfix: Tuples were inserted twice in the spatial index
- Bugfix: Fixed ConcurrentModificationException in StorageRegistry:getAllTablesForDistributionGroup
- Bugfix: Fixed forever waiting checkpoint threads, when a SSTable is switched to readonly
- Bugfix: Don't compress tuples twice
- Bugfix: Removed race condition in server socket handler

### Version 0.3.0 (Stable) - 17.06.2017
- New Feature: Added the TestFixedGrid experiment
- New Feature: Added a tuple read / write experiment
- New Feature: Added the bounding box query experiment
- New Feature: Merge SSTables based on a SortedIteratorMerger
- Improvement: Added a fixed cell data structure
- Improvement: Switched from guava 21.0 to 22.0 and from org.json 20160810 to 20170516
- Improvement: Partial written results during compactification are deleted
- Improvement: Handle thread interrupt during compactification
- Improvement: Memtables are flushed completely on shutdown()
- Improvement: Improved state handling of the SSTable manager
- Improvement: Added flush() method to SSTable manager
- Improvement: Improved ressource clean up in SSTable manager
- Improvement: Introduced the last modified timestamp for sstables
- Improvement: Big compacts are only executed every hour
- Improvement: Small table threshold is now set to 5*memtable size
- Bugfix: Don't send write requests to systems in splitting state
- Bugfix: The JVM tool options are added to the bboxdb_execute script
- Bugfix: Fixed the primary key for the JDBC osm-converter backend
- Bugfix: Name of the a invalid distribution region is removed from sstable dir
- Bugfix: Deleted tuples are now detected by bbox and value bytes
- Bugfix: Prevent the creation of tuples with 'null' data
- Bugfix: Fixed a bug in the bounding box calculation of the GeoJSON parser
- Bugfix: Fixed a bug in the query complete handler, finished queries were sent to the server
- Bugfix: Fixed some resource leaks found by coverty scan
- Bugfix: Fixed a race condition when a busy table should be redistributed
- Bugfix: Introduced the memtable flush mode. After the flush thread is stopped, all data can stay in memory
- Bugfix: Loopback IPs (e.g. 127.0.0.1) are now filtered, when the local instancename is determined

### Version 0.2.6 (Stable) - 23.05.2017
- New Feature: Created the project mailing list
- Improvement: Removed unused server read only mode
- Improvement: Send client queries immediately to server
- Improvement: Added output file to data generator
- Improvement: Synthetic data can now be generated with point bboxes
- Improvement: The execution time of the experiments is improved by using a line index
- Improvement: Renamed WeightBasedSplitStrategy to SamplingBasedSplitStrategy
- Improvement: Improved the SamplingBasedSplitStrategy by taking also object ends in consideration

### Version 0.2.5 (Stable) - 07.05.2017
- New Feature: Implemented the new insert time tuple query
- New Feature: Introduced the BBoxDB CLI
- New Feature: Added importer for GeoJSON, NYC Yellow taxi and TPCH-Lineitem formated data
- New Feature: Added generator for synthetic data
- Improvement: Added the newest inserted tuple timestamp to the Storage interface
- Improvement: Introduced the clock delta for the recovery service
- Improvement: The recovery service now uses the insert time of the tuples
- Improvement: Renamed node states to outdated and ready
- Improvement: BBoxDB uses now the server JVM
- Improvement: Improved parameter handling of the OSM data converter
- Improvement: Upgraded from Zookeeper 3.4.9 to Zookeeper 3.4.10 and from slf4j 1.7.21 to 1.7.25 and from log4j 1.2.16 to 1.2.17
- Improvement: Print connection information on failing futures
- Improvement: Added getting started page to documentation
- Improvement: Added generic execution script
- Bugfix: The checkpoint thread was waiting forever for flushed events
- Bugfix: Don’t include the content of the conf/ directory in the jar
- Bugfix: The count lines script searches in the wrong directory for source files
- Bugfix: Flush pending server responses on connection close
- Bugfix: Don’t send keep-alive packages on closing connections
- Bugfix: Fixed wrong table name in BBox queries
- Bugfix: The spatial index was not built during compactification
- Bugfix: Remove also empty memtables from unflushed table list
- Bugfix: Fixed a race condition, which prevented the deletion of flushed memtables from unflushed list
- Bugfix: Fixed bugs found by Coverity scan
- Bugfix: Process zookeeper events completely before the connection is closed

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
