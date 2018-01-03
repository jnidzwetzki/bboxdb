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
package org.bboxdb.storage.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.util.DataEncoderHelper;

import com.google.common.io.ByteStreams;

public class TupleHelper {
	
	/**
	 * Compare the tuples by key and version
	 */
	public final static Comparator<Tuple> TUPLE_KEY_AND_VERSION_COMPARATOR = 
			(t1, t2) -> {
				
				final int keyCompare = t1.getKey().compareTo(t2.getKey());
				
				if(keyCompare != 0) {
					return keyCompare;
				}
				
				return Long.compare(t1.getVersionTimestamp(), t2.getVersionTimestamp());
			};
	
	/**
	 * Return the most recent version of the tuple
	 * @param tuple1
	 * @param tuple2
	 * @return
	 */
	public static Tuple returnMostRecentTuple(final Tuple tuple1, final Tuple tuple2) {
		if(tuple1 == null && tuple2 == null) {
			return null;
		}
		
		if(tuple1 == null) {
			return tuple2;
		}
		
		if(tuple2 == null) {
			return tuple1;
		}
		
		if(tuple1.getVersionTimestamp() > tuple2.getVersionTimestamp()) {
			return tuple1;
		}
		
		return tuple2;
	}
	
	/**
	 * Write the given tuple onto the output Stream
	 * 
	 * Format of a data record:
	 * 
	 * +----------------------------------------------------------------------------------------------+
	 * | Key-Length | BBox-Length | Data-Length |  Version  |  Insert   |   Key   |  BBox   |   Data  |
	 * |            |             |             | Timestamp | Timestamp |         |         |         |
	 * |   2 Byte   |   4 Byte    |   4 Byte    |  8 Byte   |  8 Byte   |  n Byte |  n Byte |  n Byte |
	 * +----------------------------------------------------------------------------------------------+
	 * 
	 * 
	 * @param tuple
	 * @throws IOException
	 */
	public static void writeTupleToStream(final Tuple tuple, final OutputStream outputStream) throws IOException {
		final byte[] keyBytes = tuple.getKey().getBytes();
		final ByteBuffer keyLengthBytes = DataEncoderHelper.shortToByteBuffer((short) keyBytes.length);

		final byte[] boundingBoxBytes = tuple.getBoundingBoxBytes();
		final byte[] data = tuple.getDataBytes();
		
		final ByteBuffer boxLengthBytes = DataEncoderHelper.intToByteBuffer(boundingBoxBytes.length);
		final ByteBuffer dataLengthBytes = DataEncoderHelper.intToByteBuffer(data.length);
		final ByteBuffer versionTimestampBytes = DataEncoderHelper.longToByteBuffer(tuple.getVersionTimestamp());
		final ByteBuffer receivedTimestampBytes = DataEncoderHelper.longToByteBuffer(tuple.getReceivedTimestamp());

		outputStream.write(keyLengthBytes.array());
		outputStream.write(boxLengthBytes.array());
		outputStream.write(dataLengthBytes.array());
		outputStream.write(versionTimestampBytes.array());
		outputStream.write(receivedTimestampBytes.array());
		outputStream.write(keyBytes);
		outputStream.write(boundingBoxBytes);
		outputStream.write(data);
	}
	
	/**
	 * Convert the tuple into bytes
	 * @param tuple
	 * @return
	 * @throws IOException
	 */
	public static byte[] tupleToBytes(final Tuple tuple) throws IOException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		writeTupleToStream(tuple, outputStream);
		outputStream.close();
		return outputStream.toByteArray();
	}
	
	/**
	 * Decode the tuple at the current reader position
	 * 
	 * @param reader
	 * @return
	 * @throws IOException
	 */
	public static Tuple decodeTuple(final ByteBuffer byteBuffer) throws IOException {
		final short keyLength = byteBuffer.getShort();
		final int boxLength = byteBuffer.getInt();
		final int dataLength = byteBuffer.getInt();
		final long versionTimestamp = byteBuffer.getLong();
		final long receivedTimestamp = byteBuffer.getLong();

		final byte[] keyBytes = new byte[keyLength];
		byteBuffer.get(keyBytes, 0, keyBytes.length);
		
		final byte[] boxBytes = new byte[boxLength];
		byteBuffer.get(boxBytes, 0, boxBytes.length);
		
		final byte[] dataBytes = new byte[dataLength];
		byteBuffer.get(dataBytes, 0, dataBytes.length);				
		
		final String keyString = new String(keyBytes);
		
		if(isDeletedTuple(boxBytes, dataBytes)) {
			return new DeletedTuple(keyString, versionTimestamp);
		}
		
		final BoundingBox boundingBox = BoundingBox.fromByteArray(boxBytes);
		
		return new Tuple(keyString, boundingBox, dataBytes, versionTimestamp, receivedTimestamp);
	}
	
	/**
	 * Read the tuple from the input stream
	 * @param inputStream
	 * @return
	 * @throws IOException 
	 */
	public static Tuple decodeTuple(final InputStream inputStream) throws IOException {
		final byte[] keyLengthBytes = new byte[DataEncoderHelper.SHORT_BYTES];
		ByteStreams.readFully(inputStream, keyLengthBytes);
		final short keyLength = DataEncoderHelper.readShortFromByte(keyLengthBytes);
		
		final byte[] boxLengthBytes = new byte[DataEncoderHelper.INT_BYTES];
		ByteStreams.readFully(inputStream, boxLengthBytes);
		final int boxLength = DataEncoderHelper.readIntFromByte(boxLengthBytes);

		final byte[] dataLengthBytes = new byte[DataEncoderHelper.INT_BYTES];
		ByteStreams.readFully(inputStream, dataLengthBytes);
		final int dataLength = DataEncoderHelper.readIntFromByte(dataLengthBytes);

		final byte[] versionTimestampBytes = new byte[DataEncoderHelper.LONG_BYTES];
		ByteStreams.readFully(inputStream, versionTimestampBytes);
		final long versionTimestamp = DataEncoderHelper.readLongFromByte(versionTimestampBytes);

		final byte[] receivedTimestampBytes = new byte[DataEncoderHelper.LONG_BYTES];
		ByteStreams.readFully(inputStream, receivedTimestampBytes);
		final long receivedTimestamp = DataEncoderHelper.readLongFromByte(receivedTimestampBytes);

		final byte[] keyBytes = new byte[keyLength];
		ByteStreams.readFully(inputStream, keyBytes);
		
		final byte[] boxBytes = new byte[boxLength];
		ByteStreams.readFully(inputStream, boxBytes);
		
		final byte[] dataBytes = new byte[dataLength];
		ByteStreams.readFully(inputStream, dataBytes);		

		final String keyString = new String(keyBytes);

		if(isDeletedTuple(boxBytes, dataBytes)) {
			return new DeletedTuple(keyString, versionTimestamp);
		}
						
		final BoundingBox boundingBox = BoundingBox.fromByteArray(boxBytes);
		
		return new Tuple(keyString, boundingBox, dataBytes, versionTimestamp, receivedTimestamp);
	}
	
	/**
	 * Is this a deleted tuple?
	 * @param tuple
	 * @return
	 */
	public static boolean isDeletedTuple(final Tuple tuple) {
		if(tuple.getBoundingBox() == null) {
			if(Arrays.equals(tuple.getDataBytes(), SSTableConst.DELETED_MARKER)) {
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Is this a deleted tuple?
	 * @param boxBytes
	 * @param dataBytes
	 * @return
	 */
	public static boolean isDeletedTuple(final byte[] boxBytes, final byte[] dataBytes) {
		if(Arrays.equals(dataBytes,SSTableConst.DELETED_MARKER)) {
			if(Arrays.equals(boxBytes, SSTableConst.DELETED_MARKER)) {
				return true;
			}
		}
		
		return false;
	}
	
}