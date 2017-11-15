---
layout: page
title: "SSTables"
category: dev
date: 2016-12-12 12:18:27
---

# The format of the SStables

BBoxDB uses two persistent data structures. SSTables and SStableIndex files. Each file begins with a 'magic' byte sequence which is currently 'scalephant'. Files without this byte sequence are classified as invalid and not processed further.

## SStable

Format of a data record:

	+----------------------------------------------------------------------------------------------+
	| Key-Length | BBox-Length | Data-Length |  Version  |  Insert   |   Key   |  BBox   |   Data  |
	|            |             |             | Timestamp | Timestamp |         |         |         |
	|   2 Byte   |   4 Byte    |   4 Byte    |  8 Byte   |  8 Byte   |  n Byte |  n Byte |  n Byte |
	+----------------------------------------------------------------------------------------------+
	 
## SSTableIndex

Format of index records:

	+----------------------------------------------------------------+
	| Tuple-Position | Tuple-Position |  .........  | Tuple-Position |
	|                |                |             |                |
	|     4 Byte     |     4 Byte     |  .........  |     4 Byte     |
	+----------------------------------------------------------------+

	## SSTableIndex


## R-Tree spatial index

Format of a index record:

    +---------------------------------------------------------------------------------------------------+
    | Node following | Node Id | BBox-Length |    BBox   |  n x entry nodes  |  n x index node pointer  |
    |                |         |             |           |                   |                          |
    |    1 Byte      | 4 Byte  |    4 Byte   |           |     n Bytes       |       n x 4 Bytes        |
    +---------------------------------------------------------------------------------------------------+          
    
    
	 