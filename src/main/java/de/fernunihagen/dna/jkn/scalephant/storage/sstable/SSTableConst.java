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
	public final static String FILE_PREFIX = "sstable_";
	
	/**
	 * The suffix for every SSTable file
	 */
	public final static String FILE_SUFFIX = ".sst";
	
	/**
	 * The Byte order for encoded values
	 */
	public final static ByteOrder SSTABLE_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
}
