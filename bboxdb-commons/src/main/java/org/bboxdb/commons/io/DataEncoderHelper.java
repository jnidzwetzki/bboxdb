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
package org.bboxdb.commons.io;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.common.io.ByteStreams;

public class DataEncoderHelper {
	
	/**
	 * Size of a double in bytes
	 */
	public final static int DOUBLE_BYTES = Double.SIZE / Byte.SIZE;
	
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
	 * The Byte order for encoded values
	 */
	public final static ByteOrder APPLICATION_BYTE_ORDER = ByteOrder.BIG_ENDIAN;
	
	/** 
	 * Convert a array of double values into a byte buffer
	 * @param longValues
	 * @return
	 */
	public static ByteBuffer doubleArrayToByteBuffer(final double doubleValues[]) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(DOUBLE_BYTES * doubleValues.length);
		byteBuffer.order(APPLICATION_BYTE_ORDER);
		
		for(int i = 0; i < doubleValues.length; i++) {
			byteBuffer.putDouble(doubleValues[i]);
		}
		
		return byteBuffer;
	}
	
	/** 
	 * Convert a array of long values into a byte buffer
	 * @param longValues
	 * @return
	 */
	public static ByteBuffer longArrayToByteBuffer(final long longValues[]) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(LONG_BYTES * longValues.length);
		byteBuffer.order(APPLICATION_BYTE_ORDER);
		
		for(int i = 0; i < longValues.length; i++) {
			byteBuffer.putLong(longValues[i]);
		}
		
		return byteBuffer;
	}
	
	/**
	 * Encode a double into a byte buffer
	 * 
	 * @param longValue
	 * @return the long value
	 */
	public static ByteBuffer doubleToByteBuffer(final double doubleValue) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(DOUBLE_BYTES);
		byteBuffer.order(APPLICATION_BYTE_ORDER);
		byteBuffer.putDouble(doubleValue);
		return byteBuffer;
	}
	
	/**
	 * Encode a long into a byte buffer
	 * 
	 * @param longValue
	 * @return the long value
	 */
	public static ByteBuffer longToByteBuffer(final long longValue) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(LONG_BYTES);
		byteBuffer.order(APPLICATION_BYTE_ORDER);
		byteBuffer.putLong(longValue);
		return byteBuffer;
	}
	
	/**
	 * Encode an integer into a byte buffer
	 * 
	 * @param intValue
	 * @return the int value
	 */
	public static ByteBuffer intToByteBuffer(final int intValue) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(INT_BYTES);
		byteBuffer.order(APPLICATION_BYTE_ORDER);
		byteBuffer.putInt(intValue);
		return byteBuffer;		
	}
	
	/**
	 * Encode a short into a byte buffer
	 * @param shortValue
	 * @return the short value
	 */
	public static ByteBuffer shortToByteBuffer(final short shortValue) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(SHORT_BYTES);
		byteBuffer.order(APPLICATION_BYTE_ORDER);
		byteBuffer.putShort(shortValue);
		return byteBuffer;		
	}
	
	/**
	 * Decode a long array from a byte buffer
	 * @param buffer
	 * @return the long value
	 */
	public static long[] readLongArrayFromByte(final byte[] buffer) {
		final int totalValues = buffer.length / LONG_BYTES;
		long values[] = new long[totalValues];
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(APPLICATION_BYTE_ORDER);
		
		for(int i = 0; i < totalValues; i++) {
			values[i] = byteBuffer.getLong(i * LONG_BYTES);
		}
		
		return values;
	}
	
	
	/**
	 * Decode a java encoded double array from a byte buffer
	 * @param buffer
	 * @return the double value
	 */
	public static double[] readDoubleArrayFromByte(final byte[] buffer) {
		final int totalValues = buffer.length / DOUBLE_BYTES;
		final double values[] = new double[totalValues];
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(APPLICATION_BYTE_ORDER);
		
		for(int i = 0; i < totalValues; i++) {
			values[i] = byteBuffer.getDouble(i * DOUBLE_BYTES);
		}
		
		return values;
	}
	
	/**
	 * Decode a double from a byte buffer
	 * @param buffer
	 * @return the long value
	 */
	public static double readDoubleFromByte(final byte[] buffer) {
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(APPLICATION_BYTE_ORDER);
		return byteBuffer.getDouble();
	}
	
	/**
	 * Decode a long from a byte buffer
	 * @param buffer
	 * @return the long value
	 */
	public static long readLongFromByte(final byte[] buffer) {
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(APPLICATION_BYTE_ORDER);
		return byteBuffer.getLong();
	}
	
	/**
	 * Decode an int from a byte buffer
	 * @param buffer
	 * @return the int value
	 */
	public static int readIntFromByte(final byte[] buffer) {
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(APPLICATION_BYTE_ORDER);
		return byteBuffer.getInt();
	}
	
	/**
	 * Read an integer from a stream
	 * @param inputStream
	 * @return
	 * @throws IOException 
	 */
	public static int readIntFromStream(final InputStream inputStream) throws IOException {
		final byte[] elementBytes = new byte[DataEncoderHelper.INT_BYTES];
		ByteStreams.readFully(inputStream, elementBytes, 0, elementBytes.length);
		return DataEncoderHelper.readIntFromByte(elementBytes);
	}
	
	/**
	 * Read an integer from a stream
	 * @param inputStream
	 * @return
	 * @throws IOException 
	 */
	public static int readIntFromDataInput(final DataInput dataInput) throws IOException {
		final byte[] elementBytes = new byte[DataEncoderHelper.INT_BYTES];
		dataInput.readFully(elementBytes, 0, elementBytes.length);
		return DataEncoderHelper.readIntFromByte(elementBytes);
	}
	
	/**
	 * Decode a short from a byte buffer
	 * @param buffer
	 * @return the short value
	 */
	public static short readShortFromByte(final byte[] buffer) {
		final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		byteBuffer.order(APPLICATION_BYTE_ORDER);
		return byteBuffer.getShort();
	}

}
