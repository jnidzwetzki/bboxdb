package de.fernunihagen.dna.scalephant.network;

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
	 * Request type insert tuple
	 */
	public static final byte REQUEST_TYPE_INSERT_TUPLE = 0x00;
	
	/**
	 * Request type delete tuple
	 */
	public static final byte REQUEST_TYPE_DELETE_TUPLE = 0x01;
	
	/**
	 * Request type delete table
	 */
	public static final byte REQUEST_TYPE_DELETE_TABLE = 0x02;
	
	/**
	 * Request type list tables
	 */
	public static final byte REQUEST_TYPE_LIST_TABLES = 0x03;
	
	/**
	 * Request type disconnect
	 */
	public static final byte REQUEST_TYPE_DISCONNECT = 0x04;
	
	/**
	 * Request type query
	 */
	public static final byte REQUEST_TYPE_QUERY = 0x05;
	
	/**
	 * Request type transfer SSTable
	 */
	public static final byte REQUEST_TYPE_TRANSFER = 0x06;
	
	/**
	 * Request type create distribution group
	 */
	public static final byte REQUEST_TYPE_CREATE_DISTRIBUTION_GROUP = 0x07;
	
	/**
	 * Request type delete distribution group
	 */
	public static final byte REQUEST_TYPE_DELETE_DISTRIBUTION_GROUP = 0x08;
	
	/**
	 * Request network compression
	 */
	public static final byte REQUEST_TYPE_START_COMPRESSION = 0x09;
	
	/**
	 * Query type key
	 */
	public static final byte REQUEST_QUERY_KEY = 0x01;
	
	/**
	 * Query type bounding box
	 */
	public static final byte REQUEST_QUERY_BBOX = 0x02;
	
	/**
	 * Query type time
	 */
	public static final byte REQUEST_QUERY_TIME = 0x03;
	
	
	/**
	 * Response type success
	 */
	public static final byte RESPONSE_SUCCESS = 0x00;
	
	/**
	 * Response type with body
	 */
	public static final byte RESPONSE_SUCCESS_WITH_BODY = 0x01;
	
	/**
	 * Response type error
	 */
	public static final byte RESPONSE_ERROR = 0x02;
	
	/**
	 * Response type error with body
	 */
	public static final byte RESPONSE_ERROR_WITH_BODY = 0x03;
	
	/**
	 * Response of the list tables request
	 */
	public static final byte RESPONSE_LIST_TABLES = 0x04;
	
	/**
	 * Response that contains a single tuple
	 */
	public static final byte RESPONSE_TUPLE = 0x05;
	
	/**
	 * Start a multiple tuple result
	 */
	public static final byte RESPONSE_MULTIPLE_TUPLE_START = 0x06;
	
	/**
	 * End a multiple tuple result
	 */
	public static final byte RESPONSE_MULTIPLE_TUPLE_END = 0x07;

}
