/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.networkproxy.misc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.Const;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.util.TupleHelper;

import com.google.common.io.ByteStreams;

public class TupleStringSerializer {

	/**
	 * Write a tuple to the writer
	 * @param tuple
	 * @param writer
	 * @throws IOException
	 */
	public static void writeTuple(final Tuple tuple, final OutputStream outputStream) throws IOException {
		final byte[] tupleData = TupleStringSerializer.tupleToProxyBytes(tuple);

		outputStream.write(DataEncoderHelper.intToByteBuffer(tupleData.length).array());
		outputStream.write(tupleData);
	}

	/**
	 * Read tuple from reader
	 * @param reader
	 * @return
	 * @throws IOException
	 */
	public static Tuple readTuple(final InputStream reader) throws IOException {
		final int tupleLength = DataEncoderHelper.readIntFromStream(reader);
		final byte[] tupleBytes = new byte[tupleLength];

		ByteStreams.readFully(reader, tupleBytes);

		return proxyBytesToTuple(tupleBytes);
	}
	
	/**
	 * Write a joined tuple to the writer
	 * @param tuple
	 * @param writer
	 * @throws IOException
	 */
	public static void writeJoinedTuple(final MultiTuple tuple, final OutputStream outputStream) throws IOException {
		final byte[] tupleData = TupleStringSerializer.joinedTupleToProxyBytes(tuple);

		outputStream.write(DataEncoderHelper.intToByteBuffer(tupleData.length).array());
		outputStream.write(tupleData);
	}

	/**
	 * Read joined from reader
	 * @param reader
	 * @return
	 * @throws IOException
	 */
	public static MultiTuple readJoinedTuple(final InputStream reader) throws IOException {
		final int tupleLength = DataEncoderHelper.readIntFromStream(reader);
		final byte[] tupleBytes = new byte[tupleLength];

		ByteStreams.readFully(reader, tupleBytes);

		return proxyBytesToJoinedTuple(tupleBytes);
	}

	/**
	 * Convert the tuple into a string
	 * @param tuple
	 * @param sb
	 * @return
	 * @throws IOException
	 */
	public static byte[] tupleToProxyBytes(final Tuple tuple) throws IOException {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();

		final byte[] keyBytes = tuple.getKey().getBytes();
		final byte[] bboxBytes = tuple.getBoundingBox().toCompactString().getBytes();
		final byte[] dataBytes = tuple.getDataBytes();

		final int keyLength = keyBytes.length;
		final int bboxLength = bboxBytes.length;
		final int dataLength = dataBytes.length;

		bos.write(DataEncoderHelper.intToByteBuffer(keyLength).array());
		bos.write(keyBytes);
		bos.write(DataEncoderHelper.intToByteBuffer(bboxLength).array());
		bos.write(bboxBytes);
		bos.write(DataEncoderHelper.intToByteBuffer(dataLength).array());
		bos.write(dataBytes);
		bos.write(DataEncoderHelper.longToByteBuffer(tuple.getVersionTimestamp()).array());

		return bos.toByteArray();
	}
	

	/**
	 * Convert a joined tuple into the proxy representation
	 * @param joinedTuple
	 * @return
	 * @throws IOException
	 */
	public static byte[] joinedTupleToProxyBytes(final MultiTuple joinedTuple) throws IOException {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
		// Amount of tuples in the joined tuple
		final int numberOfStores = joinedTuple.getTupleStoreNames().size();
		bos.write(DataEncoderHelper.intToByteBuffer(numberOfStores).array());
		
		// Write tuple names
		for(final String tupleStoreName : joinedTuple.getTupleStoreNames()) {
			final byte[] nameBytes = tupleStoreName.getBytes(Const.DEFAULT_CHARSET);
			final int nameLength = nameBytes.length;
			bos.write(DataEncoderHelper.intToByteBuffer(nameLength).array());
			bos.write(nameBytes);
		}
		
		// Tuples
		for(final Tuple tuple : joinedTuple.getTuples()) {
			final byte[] tupleBytes = tupleToProxyBytes(tuple);
			final int tupleLength = tupleBytes.length;
			bos.write(DataEncoderHelper.intToByteBuffer(tupleLength).array());
			bos.write(tupleBytes);
		}
		
		return bos.toByteArray();
	}
	
	/**
	 * Convert the proxy string back to a tuple
	 * @param string
	 * @return
	 */
	public static Tuple proxyBytesToTuple(final byte[] bytes) throws IOException {

		try {
			if(bytes == null) {
				throw new IOException("Unable to handle a null argument");
			}

			final ByteBuffer bb = ByteBuffer.wrap(bytes);
			bb.order(Const.APPLICATION_BYTE_ORDER);

			// Key
			final int keyLength = bb.getInt();
			final byte[] keyBytes = new byte[keyLength];
			bb.get(keyBytes, 0, keyBytes.length);
			final String key = new String(keyBytes);

			// Bounding box
			final int bboxLength = bb.getInt();
			final byte[] bboxBytes = new byte[bboxLength];
			bb.get(bboxBytes, 0, bboxBytes.length);
			final String bboxString = new String(bboxBytes);
			final Hyperrectangle bbox = Hyperrectangle.fromString(bboxString);

			// Value
			final int valueLength = bb.getInt();
			final byte[] valueBytes = new byte[valueLength];
			bb.get(valueBytes, 0, valueBytes.length);

			// Version timestamp
			final long timestamp = bb.getLong();

			if(bb.hasRemaining()) {
				throw new IOException("Buffer has remaining bytes");
			}

			if(TupleHelper.isDeletedTuple(bbox, valueBytes)) {
				return new DeletedTuple(key, timestamp);
			} else {
				return new Tuple(key, bbox, valueBytes, timestamp);
			}

		} catch (BufferUnderflowException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Convert the proxy bytes back into a joined tuple
	 * @param bytes
	 * @return
	 * @throws IOException
	 */
	public static MultiTuple proxyBytesToJoinedTuple(final byte[] bytes) throws IOException {
		try {
			if(bytes == null) {
				throw new IOException("Unable to handle a null argument");
			}
	
			final ByteBuffer bb = ByteBuffer.wrap(bytes);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			
			final int amountOfEntries = bb.getInt();
			
			// Read tuple store names
			final List<String> tupleStoreNames = new ArrayList<>();
			for(int i = 0; i < amountOfEntries; i++) {
				final int nameLength = bb.getInt();
				final byte[] nameBytes = new byte[nameLength];
				bb.get(nameBytes, 0, nameBytes.length);
				final String tableName = new String(nameBytes);
				tupleStoreNames.add(tableName);
			}
			
			// Read tuples
			final List<Tuple> tupleList = new ArrayList<>();
			for(int i = 0; i < amountOfEntries; i++) {
				final int tupleLength = bb.getInt();
				final byte[] tupleBytes = new byte[tupleLength];
				bb.get(tupleBytes, 0, tupleBytes.length);
				final Tuple tuple = proxyBytesToTuple(tupleBytes);
				tupleList.add(tuple);
			}
			
			return new MultiTuple(tupleList, tupleStoreNames);
		} catch (BufferUnderflowException e) {
			throw new IOException(e);
		}
	}

}
