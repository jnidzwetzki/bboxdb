package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.nio.ByteBuffer;

public class SSTableHelper {
	
	/**
	 * Size of a Long in Bytes
	 */
	public final static int LONG_BYTES = Long.SIZE / Byte.SIZE;
	
	/**
	 * Shize of a Integer in Bytes
	 */
	public final static int INT_BYTES = Integer.SIZE / Byte.SIZE;
	
	/**
	 * Size of a Short in Bytes
	 */
	public final static int SHORT_BYTES = Short.SIZE / Byte.SIZE;
	
	
	
	/**
	 * Encode a long into a byte buffer
	 * 
	 * @param longValue
	 * @return the long value
	 */
	public static ByteBuffer longToByteBuffer(long longValue) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(LONG_BYTES);
		byteBuffer.order(SSTableConst.SSTABLE_BYTE_ORDER);
		byteBuffer.putLong(longValue);
		return byteBuffer;
	}
	
	/**
	 * Encode an integer into a byte buffer
	 * 
	 * @param intValue
	 * @return the int value
	 */
	public static ByteBuffer intToByteBuffer(int intValue) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(INT_BYTES);
		byteBuffer.order(SSTableConst.SSTABLE_BYTE_ORDER);
		byteBuffer.putInt(intValue);
		return byteBuffer;		
	}
	
	/**
	 * Encode a short into a byte buffer
	 * @param shortValue
	 * @return the short value
	 */
	public static ByteBuffer shortToByteBuffer(short shortValue) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(SHORT_BYTES);
		byteBuffer.order(SSTableConst.SSTABLE_BYTE_ORDER);
		byteBuffer.putShort(shortValue);
		return byteBuffer;		
	}
	
	/**
	 * Decode a long from a byte buffer
	 * @param buffer
	 * @return the long value
	 */
	public static long readLongFromByteBuffer(byte[] buffer) {
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(SSTableConst.SSTABLE_BYTE_ORDER);
		return byteBuffer.getLong();
	}
	
	/**
	 * Decode an int from a byte buffer
	 * @param buffer
	 * @return the int value
	 */
	public static int readIntFromByteBuffer(byte[] buffer) {
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(SSTableConst.SSTABLE_BYTE_ORDER);
		return byteBuffer.getInt();
	}
	
	/**
	 * Decode a short from a byte buffer
	 * @param buffer
	 * @return the short value
	 */
	public static short readShortFromByteBuffer(byte[] buffer) {
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(SSTableConst.SSTABLE_BYTE_ORDER);
		return byteBuffer.getShort();
	}

}
