# The network protocol of the scalephant

The protocol of the scalephant is based on frames. Each frame consists of a header and a body. The header has a fixed size, the body has a variable size. It exists two types of frames: request and response frames. The request frame is send from the client to the scalephant, the response frame is send from the scalephant to the client.

## The request frame

    0         8       16       24       32
	+---------+--------+--------+--------+
	| Version |    Request-ID   |  Type  |
	+---------+-----------------+--------+
	|            Body-Length             |
	+------------------------------------+
	|                                    |
	|               Body                 |
	.                                    .
	.                                    .
	+------------------------------------+
 
### Header

* Version - The protocol version, currently always 0x01.
* Request-ID - The id of the request, e.g., a consecutive number.
* Type - The type of the request.

Request Types:

* Type 0x00 - Insert tuple request
* Type 0x01 - Delete tuple request
* Type 0x02 - Truncate table request
* Type 0x03 - Query request

## Body
The structure of the body depends on the request type. The next sections describe the used structures.

#### Insert
This package inserts a new tuple into a given table. 

###### Request Body

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
	

###### Response 

#### Delete
This package deletes a tuple from a table.

#### Truncate
Truncate a whole table

#### Query



