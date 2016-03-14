# scalephant - a distributed and highly scalable Key-Value Store based on SSTables

SSTables (String Sorted Tables) are used nowadays in many NoSQL databases like Cassandra or LevelDB. SSTables provide a high throughput for read and write operations. scalephant is a java implementation of SSTables. I wrote this software for research purposes and to understand some of the techniques that are used in other implementations.

[![Build Status](https://travis-ci.org/jnidzwetzki/scalephant.svg?branch=master)](https://travis-ci.org/jnidzwetzki/scalephant)


## Changelog

### Version 0.0.5 (Alpha) - TBA
- Improved SSTable usage counting
- Added SStable metadata
- Added distributed membership management
- Added the cluster management script
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
