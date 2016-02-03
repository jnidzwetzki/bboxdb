package de.fernunihagen.dna.jkn.scalephant.network;

public class NetworkConst {
	
	/**
	 * The version of the network protocol
	 */
	public static final int PROTOCOL_VERSION = 0x01;
	
	/**
	 * Request type insert tuple
	 */
	public static final int REQUEST_TYPE_INSERT_TUPLE = 0x00;
	
	/**
	 * Request type delete tuple
	 */
	public static final int REQUEST_TYPE_DELETE_TUPLE = 0x01;
	
	/**
	 * Request type delete table
	 */
	public static final int REQUEST_TYPE_DELETE_TABLE = 0x02;
	
	/**
	 * Request type list tables
	 */
	public static final int REQUEST_TYPE_LIST_TABLES = 0x03;
	
	/**
	 * Request type query
	 */
	public static final int REQUEST_TYPE_QUERY = 0x04;
	
}
