# The network protocol of the scalephant

The protocol of the scalephant is based on frames. Each frame consists of a header and a body. The header has a fixed size, the body has a variable size. It exists two types of frames: request and response frames. The request frame is send from the client to the scalephant, the response frame is send from the scalephant to the client.

## The request frame

    0         8       16       24       32
	+---------+--------+--------+--------+
	| Version |  Type  |   Request-ID    |
	+---------+-----------------+--------+
	|            Body-Length             |
	+------------------------------------+
	|                                    |
	|               Body                 |
	.                                    .
	.                                    .
	+------------------------------------+
 
### Request Header

* Version - The protocol version, currently always 0x01.
* Type - The type of the request.
* Request-ID - The id of the request, e.g., a consecutive number.

Request Types:

* Type 0x00 - Insert tuple request
* Type 0x01 - Delete tuple request
* Type 0x02 - Delete table request
* Type 0x03 - List all tables request
* Type 0x04 - Disconnect request
* Type 0x05 - Query request


## The response frame

    0         8       16       24       32
	+---------+--------+--------+--------+
	| Version |    Request-ID   | Result |
	+---------+-----------------+--------+
	
* Version - The protocol version, currently always 0x01.
* Request-ID - The id of the request which the repsonse belongs too.
* Result - The result of the operation

Result-Types:

* Type 0x00 - Operation Success - no package body
* Type 0x01 - Operation Success - with details in the body
* Type 0x02 - Operation Error - no package body
* Type 0x03 - Operation Error - with details in the body
	
### Body for response type = 0x03 (Error with details)

    0         8       16       24       32
	+---------+--------+--------+--------+
	|   Message-Length |     Message     |
	+------------------+                 |
	.                                    .
	.                                    .
	+------------------------------------+
	
* Message-Length - The length of the error message
* Message - The error message

## Frame body
The structure of the body depends on the request type. The next sections describe the used structures.

### Insert
This package inserts a new tuple into a given table. The result could be currently response type 0x00, 0x02 and 0x03.

#### Request body

    0         8       16       24       32
	+---------+--------+--------+--------+
	|   Table-Length   |   Key-Length    |
	+------------------+-----------------+
	|            BBox-Length             |
	+------------------------------------+
	|            Data-Length             |
	+------------------------------------|
	|             Timestamp              |
	|                                    |
	+------------------------------------+
	|             Tablename              |
	.                                    .
	+------------------------------------+
	|               Key                  |
	.                                    .
	+------------------------------------+
	|               BBOX                 |
	.                                    .
	+------------------------------------+
	|                                    |
	|               Data                 |
	.                                    .
	.                                    .
	+------------------------------------+
	

### Delete Tuple
This package deletes a tuple from a table. The result could be currently response type 0x00, 0x02 and 0x03.

#### Request body

    0         8       16       24       32
	+---------+--------+--------+--------+
	|   Table-Length   |   Key-Length    |
	+------------------+-----------------+
	|              Tablename             |
	.                                    .
	+------------------------------------+
	|                 Key                |
	.                                    .
	+------------------------------------+
	
### Delete Table
This package deletes a whole table. The result could be currently response type 0x00, 0x02 and 0x03.

#### Request body

    0         8       16       24       32
	+---------+--------+--------+--------+
	|   Table-Length   |                 |
	+------------------+                 |
	|              Tablename             |
	.                                    .
	+------------------------------------+
	

### List all tables
This package lists all existing tables

#### Request body

The body of the package is empty

    0         8       16       24       32
	+---------+--------+--------+--------+

#### Response body
The request body contains the names of the existing tables, seperated by a terminal symbol (\0).

    0         8       16       24       32
	+---------+--------+--------+--------+
	|         Length of all Tables       |
	+---------+--------+--------+--------+
	|       Table1\0|   Table2\0|Table3\0|
	+----------------+----------+--------+
	|        Table4\0|
	+----------------+
	


### Disconnect 
Discconnect from server

#### Request body

The body of the package is empty

    0         8       16       24       32
	+---------+--------+--------+--------+

#### Response body
The result could be currently only response type 0x00. The server waits until all pending operations are completed successfully. Afterwards, the reponse type 0x00 is send and the connection is closed. 

### Query
This package represents a query.  

#### Request body

Query type:

* Type 0x01 - Key query
* Type 0x02 - Bounding Box query

#### Response body

