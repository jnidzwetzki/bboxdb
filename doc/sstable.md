# The format of the SStables

The Scalephant uses two persistent data structures. SSTables and SStableIndex files. Each file begins with a 'magic' byte sequence which is currently 'scalephant'. Files without this byte sequene are classified as invalid and not processed further.

## SStable

Format of a data record:

	+--------------------------------------------------------------------------------+
	| Key-Length | BBox-Length | Data-Length | Timestamp |   Key   |  BBox  |  Data  |
	|   2 Byte   |    4 Byte   |    4 Byte   |   8 Byte  |         |        |        |
	+--------------------------------------------------------------------------------+
	 
## SSTableIndex

Format of index records:

	+-----------------------------------------------+
	| Tuple-Position | Tuple-Position |  .........  |
	|     4 Byte     |     4 Byte     |  .........  |
	+-----------------------------------------------+
