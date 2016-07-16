package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.nio.ByteOrder;

public class SSTableConst {
	
	/**
	 * The magic bytes at the beginning of every SSTable
	 */
	public final static byte[] MAGIC_BYTES = "scalephant".getBytes();
	
	/**
	 * The current version of the SSTable layout format
	 */
	public final short SST_VERSION = 1;
	
	/**
	 * The prefix for every SSTable file
	 */
	public final static String SST_FILE_PREFIX = "sstable_";
	
	/**
	 * The suffix for every SSTable file
	 */
	public final static String SST_FILE_SUFFIX = ".sst";
	
	/**
	 * The suffix for every SSTable index file
	 */
	public final static String SST_INDEX_SUFFIX = ".idx";
	
	/**
	 * The suffix for the meta files
	 */
	public final static String SST_META_SUFFIX = ".meta";
	
	/**
	 * The Byte order for encoded values
	 */
	public final static ByteOrder SSTABLE_BYTE_ORDER = ByteOrder.BIG_ENDIAN;
	
	/**
	 * Format of the index file:
	 * 
	 * -------------------------------------------------
	 * | Tuple-Position | Tuple-Position |  .........  |
	 * |     4 Byte     |     4 Byte     |  .........  |
	 * -------------------------------------------------
	 */
	public final static int INDEX_ENTRY_BYTES = 4;
	
	/**
	 * Marker for deleted tuples
	 */
	public final static byte[] DELETED_MARKER = "DEL".getBytes();
	
	/**
	 * Execution interval for the comptact thread (30 seconds)
	 */
	public final static int COMPACT_THREAD_DELAY = 30 * 1000;
}
