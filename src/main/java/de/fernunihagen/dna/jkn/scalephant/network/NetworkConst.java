package de.fernunihagen.dna.jkn.scalephant.network;

import java.nio.ByteOrder;

public class NetworkConst {
	
	/**
	 * The version of the network protocol
	 */
	public static final byte PROTOCOL_VERSION = 0x01;
	
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
	 * Byte order for network communication
	 */
	public static final ByteOrder NETWORK_BYTEORDER = ByteOrder.BIG_ENDIAN;
	
}
