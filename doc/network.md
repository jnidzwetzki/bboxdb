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
* Type 0x04 - Query request


## The response frame

    0         8       16       24       32
	+---------+--------+--------+--------+
	| Version |    Request-ID   | Result |
	+---------+-----------------+--------+
	

## Frame body
The structure of the body depends on the request type. The next sections describe the used structures.

### Insert
This package inserts a new tuple into a given table. 

#### Request body

    0         8       16       24       32
	+---------+--------+--------+--------+
	|       Table      |    Key-Length   |
	+------------------+-----------------+
	|            BBox-Length             |
	+------------------------------------+
	|            Data-Length             |
	+------------------------------------+
	|             Timestamp              |
	|                                    |
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
	

#### Response body

### Delete Tuple
This package deletes a tuple from a table.

#### Request body

    0         8       16       24       32
	+---------+--------+--------+--------+
	|       Table      |    Key-Length   |
	+------------------------------------+
	|               Key                  |
	.                                    .
	+------------------------------------+
	
#### Response body


### Delete Table
This package deletes a whole table

#### Request body

    0         8       16       24       32
	+---------+--------+--------+--------+
	|       Table      |      Unused     |
	+------------------------------------+
	
#### Response body


### List all tables
This package lists all existing tables

#### Request body

    0         8       16       24       32
	+---------+--------+--------+--------+
	|                Unused              | 
	+------------------------------------+

#### Response body


### Query
This package represents a query.  

#### Request body

Query type:

* Type 0x01 - Key query
* Type 0x02 - Bounding Box query

#### Response body

