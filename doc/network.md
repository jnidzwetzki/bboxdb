# The network protocol of the scalephant

The protocol of the scalephant is based on frames. Each frame consists of a header and a body. The header has a fixed size, the body has a variable size. It exists two types of frames: request and response frames. The request frame is send from the client to the scalephant, the response frame is send from the scalephant to the client.

## The request frame

    0         8       16       24       32
	+---------+--------+--------+--------+
	|       Type       |   Request-ID    |
	+---------+--------+-----------------+
	|            Body-Length             |
	|                                    |
	+---------+-----------------+--------+
	| Routed  |       Hop       | Unused |
	+---------+--------+--------+--------+
	|  Length of hosts |  Routing-List   |
	+------------------+-----------------+
	|                                    |
	|               Body                 |
	.                                    .
	.                                    .
	+------------------------------------+
 
### Request Header

* Type - The type of the request.
* Request-ID - The id of the request, e.g., a consecutive number.
* Body length - The length of the body as a long value.
* Routed - Does the package contain routing information (0x01) or not (0x0).
* Hop - The hop of the package. Is set to 0x00 if the package is not routed.
* Length of host - The length of the host list. Will be set to 0x00 if the package is not routed.
* Routing-List - A comma separated list of hosts for package routing. The format of the list is: [host1:port,host2:port,...].

Request Types:

* Type 0x00 - Helo
* Type 0x01 - Insert tuple request
* Type 0x02 - Delete tuple request
* Type 0x03 - Delete table request
* Type 0x04 - List all tables request
* Type 0x05 - Disconnect request
* Type 0x06 - Query request
* Type 0x07 - Transfer SSTable
* Type 0x08 - Create distribution group
* Type 0x09 - Delete distribution group


## The response frame

    0         8       16       24       32
	+---------+--------+--------+--------+
	|     Request-ID   |  Result-Type    | 
	+---------+-----------------+--------+
	|             Body length            |
	|                                    |
	+------------------------------------+
	|                                    |
	|               Body                 |
	.                                    .
	.                                    .
	+------------------------------------+
	
* Request-ID - The id of the request which the response belongs too.
* Result-Type - The result type of the operation.
* Body length - The length of the body as a long value. For Packages without body, the length is set to 0.

Result-Types:

* Type 0x00 - Helo result
* Type 0x01 - Operation Success - no package body
* Type 0x02 - Operation Success - with details in the body
* Type 0x03 - Operation Error - no package body
* Type 0x04 - Operation Error - with details in the body
* Type 0x05 - Result of the List tables call
* Type 0x06 - A result that contains a tuple
* Type 0x07 - Start multiple tuple result
* Type 0x08 - End multiple tuple result

	
### Body for response type = 0x01/0x03 (Success/Error with details)

    0         8       16       24       32
	+---------+--------+--------+--------+
	|   Message-Length |     Message     |
	+------------------+                 |
	.                                    .
	.                                    .
	+------------------------------------+
	
* Message-Length - The length of the error message
* Message - The error message

### Body for response type = 0x05
This is a response body that contains a tuple.

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
	
Note: All time stamps are 64 bit long and have a resolution of nano seconds
	
### Body for response type = 0x06 / 0x07
By using the response types 0x06 and 0x07 a set of tuples can be transfered. For example, this could be the result of a query. The begin of the transfer of the tuple set is indicated by the package type 0x06; the end is indicated by the type 0x07. Both package types have an empty body. 

Transferring a set of tuples:

    +-------------------------------------+
    |  0x06 - Start multiple tuple result |
    +-------------------------------------+
    |  0x05 - A result tuple              |
    +-------------------------------------+
    |  0x05 - A result tuple              |
    +-------------------------------------+
    |               ....                  |
    +-------------------------------------+
	|  0x05 - A result tuple              |
    +-------------------------------------+
	|  0x07 - End multiple tuple result   |
    +-------------------------------------+
    
## Frame body
The structure of the body depends on the request type. The next sections describe the used structures.

### Helo 
Handshake with the server

#### Request body

The body contains the protocol version and the features of the client.

    0         8       16       24       32
	+---------+--------+--------+--------+
	|          Protocol Version          |
	+------------------------------------+
	|           Client-Features          |
	+------------------------------------+
	
Client features:

Bit 0: Compression

#### Response body
The body contains the protocol version and the features of the server.

    0         8       16       24       32
	+---------+--------+--------+--------+
	|          Protocol Version          |
	+------------------------------------+
	|           Client-Features          |
	+------------------------------------+
	
Client features:

Bit 0: Compression

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
The response body contains the names of the existing tables.

    0         8       16       24       32
	+---------+--------+--------+--------+
    |           Number of tables         | 
    +------------------+-----------------+
    | Length of table 1|   Table name 1  |
    +------------------+-----------------+
    |                 ...                |
    +------------------+-----------------+   
    | Length of table n|   Table name n  |
    +------------------------------------+  
    
### Disconnect 
Disconnect from server

#### Request body

The body of the package is empty

    0         8       16       24       32
	+---------+--------+--------+--------+

#### Response body
The result could be currently only response type 0x00. The server waits until all pending operations are completed successfully. Afterwards, the response type 0x00 is send and the connection is closed. 

### Query
This package represents a query.

#### Request body

The request body of a query consists of the query type and specific data for the particular query.

    0         8       16       24       32
	+---------+--------+--------+--------+
	| Q-Type  |                          | 
	+---------+                          |
	|              Query-Data            |
	.                                    .
	+------------------------------------+

Query type:

* Type 0x01 - Key query
* Type 0x02 - Bounding Box query
* Type 0x03 - Time query

### Key-Query
This query asks for a specific key in a particular table.

#### Request body

    0         8       16       24       32
	+---------+--------+--------+--------+
	|  0x01   |  Table-Length   | Key-   | 
	+---------+-----------------+--------+
	| -Length |     Tablename            |
	+---------+                          |
	.                                    .
	+------------------------------------+
	|                 Key                |
	.                                    .
	+------------------------------------+


#### Response body
The result could be currently the response types 0x00, 0x02, 0x03 and 0x05. The result types 0x02, 0x03 indicate an error. The result type 0x00 means, that the query is processed successfully, but no matching tuple was found. The result type 0x05 indicates that one tuple is found.

### Bounding-Box-Query
This query asks for all tuples, that are covered by the bounding box.

#### Request body

    0         8       16       24       32
	+---------+--------+--------+--------+
	|  0x02   | Unused |  Table-Length   |
	+---------+--------------------------+
	|              BBOX-Length           | 
	+------------------------------------+ 
	|              Tablename             |
	.                                    .
	+------------------------------------+
	|                 BBOX               |
	.                                    .
	+------------------------------------+

#### Response body
The result could be currently the response types 0x02, 0x03 and 0x06.

### Time-Query
This query asks for all tuples, that are inserted after certain time stamp (time(tuple) > time stamp).

#### Request body

    0         8       16       24       32
	+---------+--------+--------+--------+
	|  0x03   |          Unused          |
    +---------+--------------------------+
	|              Timestamp             |
    |                                    |
    +-----------------+------------------+
	|   Table-length  |                  |
	+-----------------+                  |
	|              Tablename             |
    +------------------------------------+
    
#### Response body
The result could be currently the response types 0x02, 0x03 and 0x06.

### Transfer SSTable
This request transfers a whole SSTable from one instance to another. This request is send between two scalephant instances. 

#### Request body

    0         8       16       24       32
	+---------+--------+--------+--------+
	|   Table-Length   |      Unused     |
	+------------------+-----------------+
	|          Metadata-Length           |
	|                                    |
	+------------------------------------+	
	|           SSTable-Length           |
	|                                    |
	+------------------------------------+
	|          Keyindex-Length           |
	|                                    |
	+------------------------------------+
	|              Tablename             |
	.                                    .
	+------------------------------------+
	|              Metadata              |
	|                                    |
	.                                    .
	+------------------------------------+
	|               SSTable              |
	|                                    |
	.                                    .
	+------------------------------------+
	|              Keyindex              |
	|                                    |
	.                                    .
	+------------------------------------+
	
#### Response body
The result could be currently the response types 0x00, 0x02 or the receiving server closes the tcp socket during the file transfer. 


### Create distribution group
This package deletes a whole table. The result could be currently response type 0x00, 0x02 and 0x03.

#### Request body

    0         8       16       24       32
	+---------+--------+--------+--------+
	|   Group-Length   |   Replication   |
	+------------------+-----------------+
	|        Distribution Group          |
	.                                    .
	+------------------------------------+
	
The field 'replication' determines how many replicates are created into the distribution group

### Delete distribution group
This package deletes a whole table. The result could be currently response type 0x00, 0x02 and 0x03.

#### Request body
    0         8       16       24       32
	+---------+--------+--------+--------+
	|   Group-Length   |                 |
	+------------------+                 |
	|        Distribution Group          |
	.                                    .
	+------------------------------------+




