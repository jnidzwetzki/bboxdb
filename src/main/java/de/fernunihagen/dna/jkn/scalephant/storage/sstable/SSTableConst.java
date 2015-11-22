package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.nio.ByteOrder;

public class SSTableConst {
	
	/**
	 * The magic bytes at the beginning of every SSTable
	 */
	public final static byte[] MAGIC_BYTES = "scalephant".getBytes();
	
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
	 * The Byte order for encoded values
	 */
	public final static ByteOrder SSTABLE_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
	
	/**
	 * Format of the index file:
	 * 
	 * -------------------------------------------------
	 * | Tuple-Position | Tuple-Position |  .........  |
	 * |     8 Byte     |     8 Byte     |  .........  |
	 * -------------------------------------------------
	 */
	public final static int INDEX_ENTRY_BYTES = 8;
}
