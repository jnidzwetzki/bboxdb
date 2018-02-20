/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
package org.bboxdb.network;

public class NetworkConst {
	
	/**
	 * The version of the network protocol
	 */
	public static final byte PROTOCOL_VERSION = 0x01;
	
	/**
	 * Value of an unused byte
	 */
	public final static byte UNUSED_BYTE = 0;
	
	/**
	 * Request network compression
	 */
	public static final short REQUEST_TYPE_HELLO = 0x00;
	
	/**
	 * Request type insert tuple
	 */
	public static final short REQUEST_TYPE_INSERT_TUPLE = 0x01;
	
	/**
	 * Create a new table
	 */
	public static final short REQUEST_TYPE_CREATE_TABLE = 0x03;
	
	/**
	 * Request type delete table
	 */
	public static final short REQUEST_TYPE_DELETE_TABLE = 0x04;
	
	/**
	 * Request type list tables
	 */
	public static final short REQUEST_TYPE_LIST_TABLES = 0x05;
	
	/**
	 * Request type disconnect
	 */
	public static final short REQUEST_TYPE_DISCONNECT = 0x06;
	
	/**
	 * Request type query
	 */
	public static final short REQUEST_TYPE_QUERY = 0x07;
	
	/**
	 * Request type create distribution group
	 */
	public static final short REQUEST_TYPE_CREATE_DISTRIBUTION_GROUP = 0x08;
	
	/**
	 * Request type delete distribution group
	 */
	public static final short REQUEST_TYPE_DELETE_DISTRIBUTION_GROUP = 0x09;

	/**
	 * Compression envelope
	 */
	public static final short REQUEST_TYPE_COMPRESSION = 0x10;
	
	/**
	 * Keep alive package
	 */
	public static final short REQUEST_TYPE_KEEP_ALIVE = 0x11;
	
	/**
	 * Next page package
	 */
	public static final short REQUEST_TYPE_NEXT_PAGE = 0x12;
	
	/**
	 * Cancel the given query
	 */
	public static final short REQUEST_TYPE_CANCEL_QUERY = 0x13;
	
	/**
	 * Query type key
	 */
	public static final byte REQUEST_QUERY_KEY = 0x01;
	
	/**
	 * Query type bounding box
	 */
	public static final byte REQUEST_QUERY_BBOX = 0x02;
	
	/**
	 * Query type version time
	 */
	public static final byte REQUEST_QUERY_VERSION_TIME = 0x03;
	
	/**
	 * Query type insert time
	 */
	public static final byte REQUEST_QUERY_INSERT_TIME = 0x04;
	
	/**
	 * Query type bounding box and Time
	 */
	public static final byte REQUEST_QUERY_BBOX_AND_TIME = 0x05;

	/**
	 * Query type continuous bounding box
	 */
	public static final byte REQUEST_QUERY_CONTINUOUS_BBOX = 0x06;
	
	/**
	 * Query type join
	 */
	public static final byte REQUEST_QUERY_JOIN = 0x07;
	
	/**
	 * Response type helo
	 */
	public static final short RESPONSE_TYPE_HELLO = 0x00;
	
	/**
	 * Response type with body
	 */
	public static final short RESPONSE_TYPE_SUCCESS = 0x01;

	/**
	 * Response type error with body
	 */
	public static final short RESPONSE_TYPE_ERROR = 0x02;
	
	/**
	 * Response of the list tables request
	 */
	public static final short RESPONSE_TYPE_LIST_TABLES = 0x03;
	
	/**
	 * Response that contains a single tuple
	 */
	public static final short RESPONSE_TYPE_TUPLE = 0x04;
	
	/**
	 * Start a multiple tuple result
	 */
	public static final short RESPONSE_TYPE_MULTIPLE_TUPLE_START = 0x05;
	
	/**
	 * End a multiple tuple result
	 */
	public static final short RESPONSE_TYPE_MULTIPLE_TUPLE_END = 0x06;
	
	/**
	 * Page has ended
	 */
	public static final short RESPONSE_TYPE_PAGE_END = 0x07;
	
	/**
	 * Page has ended
	 */
	public static final short RESPONSE_TYPE_JOINED_TUPLE = 0x08;
	
	/**
	 * Compression envelope request
	 */
	public static final short RESPONSE_TYPE_COMPRESSION = 0x10;

	
	/**
	 * The gzip compression type
	 */
	public final static byte COMPRESSION_TYPE_GZIP = 0x00;
	

	/**
	 * The thread wakeup time (100 ms) to flush the pending compression packages
	 * to the server
	 */
	public final static long MAX_COMPRESSION_DELAY_MS = 100;
	
}
