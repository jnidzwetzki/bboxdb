package de.fernunihagen.dna.scalephant;

import java.nio.ByteOrder;

public class Const {
	
	/**
	 *  The version of the software
	 */
	public final static String VERSION = "0.1.2";
	
	/**
	 * The name of the configuration file
	 */
	public final static String CONFIG_FILE = "scalephant.yaml";
	
	/**
	 * If an operation is retried, this is the max amount
	 * of retries
	 */
	public final static int OPERATION_RETRY = 10;
	
	/**
	 * The Byte order for encoded values
	 */
	public final static ByteOrder APPLICATION_BYTE_ORDER = ByteOrder.BIG_ENDIAN;
	
}
