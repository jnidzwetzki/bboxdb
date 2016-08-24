package de.fernunihagen.dna.scalephant.tools;

import java.nio.ByteBuffer;

import de.fernunihagen.dna.scalephant.Const;

public class DataEncoderHelper {
	
	/**
	 * Size of a float in bytes
	 */
	public final static int FLOAT_BYTES = Float.SIZE / Byte.SIZE;
	
	/**
	 * Size of a IEEE 754 encoded float in bytes
	 */
	public final static int FLOAT_IEEE754_BYTES = Integer.SIZE / Byte.SIZE;
	
	/**
	 * Size of a long in bytes
	 */
	public final static int LONG_BYTES = Long.SIZE / Byte.SIZE;
	
	/**
	 * Size of a integer in bytes
	 */
	public final static int INT_BYTES = Integer.SIZE / Byte.SIZE;
	
	/**
	 * Size of a short in bytes
	 */
	public final static int SHORT_BYTES = Short.SIZE / Byte.SIZE;
	
	
	/** 
	 * Convert a array of long values into a byte buffer
	 * @param longValues
	 * @return
	 */
	public static ByteBuffer longArrayToByteBuffer(long longValues[]) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(LONG_BYTES * longValues.length);
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
		
		for(int i = 0; i < longValues.length; i++) {
			byteBuffer.putLong(longValues[i]);
		}
		
		return byteBuffer;
	}
	
	/** 
	 * Convert a array of float values into a byte buffer (in IEEE 754 notation)
	 * @param longValues
	 * @return
	 */
	public static ByteBuffer floatArrayToIEEE754ByteBuffer(float floatValues[]) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(FLOAT_IEEE754_BYTES * floatValues.length);
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
		
		for(int i = 0; i < floatValues.length; i++) {
			byteBuffer.putInt(Float.floatToIntBits(floatValues[i]));
		}
		
		return byteBuffer;
	}
	
	/** 
	 * Convert a array of float values into a byte buffer (in java notation)
	 * @param longValues
	 * @return
	 */
	public static ByteBuffer floatArrayToByteBuffer(float floatValues[]) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(FLOAT_BYTES * floatValues.length);
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
		
		for(int i = 0; i < floatValues.length; i++) {
			byteBuffer.putFloat(floatValues[i]);
		}
		
		return byteBuffer;
	}
	
	/**
	 * Encode a long into a byte buffer
	 * 
	 * @param longValue
	 * @return the long value
	 */
	public static ByteBuffer longToByteBuffer(long longValue) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(LONG_BYTES);
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
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
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
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
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
		byteBuffer.putShort(shortValue);
		return byteBuffer;		
	}
	
	/**
	 * Decode a long array from a byte buffer
	 * @param buffer
	 * @return the long value
	 */
	public static long[] readLongArrayFromByte(byte[] buffer) {
		final int totalValues = buffer.length / LONG_BYTES;
		long values[] = new long[totalValues];
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
		
		for(int i = 0; i < totalValues; i++) {
			values[i] = byteBuffer.getLong(i * LONG_BYTES);
		}
		
		return values;
	}
	
	/**
	 * Decode a IEEE 754 encoded float array from a byte buffer
	 * @param buffer
	 * @return the float value
	 */
	public static float[] readIEEE754FloatArrayFromByte(byte[] buffer) {
		final int totalValues = buffer.length / FLOAT_IEEE754_BYTES;
		float values[] = new float[totalValues];
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
		
		for(int i = 0; i < totalValues; i++) {
			final int value = byteBuffer.getInt(i * FLOAT_IEEE754_BYTES);
			values[i] = Float.intBitsToFloat(value);
		}
		
		return values;
	}
	
	/**
	 * Decode a java encoded float array from a byte buffer
	 * @param buffer
	 * @return the float value
	 */
	public static float[] readFloatArrayFromByte(byte[] buffer) {
		final int totalValues = buffer.length / FLOAT_BYTES;
		float values[] = new float[totalValues];
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
		
		for(int i = 0; i < totalValues; i++) {
			values[i] = byteBuffer.getFloat(i * FLOAT_BYTES);
		}
		
		return values;
	}
	
	/**
	 * Decode a long from a byte buffer
	 * @param buffer
	 * @return the long value
	 */
	public static long readLongFromByte(byte[] buffer) {
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
		return byteBuffer.getLong();
	}
	
	/**
	 * Decode an int from a byte buffer
	 * @param buffer
	 * @return the int value
	 */
	public static int readIntFromByte(byte[] buffer) {
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
		return byteBuffer.getInt();
	}
	
	/**
	 * Decode a short from a byte buffer
	 * @param buffer
	 * @return the short value
	 */
	public static short readShortFromByte(byte[] buffer) {
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
		return byteBuffer.getShort();
	}

}
