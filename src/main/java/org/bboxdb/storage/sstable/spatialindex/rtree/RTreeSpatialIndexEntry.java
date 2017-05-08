/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.storage.sstable.spatialindex.rtree;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.util.DataEncoderHelper;

import com.google.common.io.ByteStreams;

public class RTreeSpatialIndexEntry extends SpatialIndexEntry {

	/**
	 * The id of the node
	 */
	protected final int nodeId;
	
	public RTreeSpatialIndexEntry(final int nodeId, final SpatialIndexEntry spatialIndexEntry) {
		super(spatialIndexEntry.getKey(), spatialIndexEntry.getBoundingBox());
		this.nodeId = nodeId;
	}
	
	public RTreeSpatialIndexEntry(final int nodeId, final String key, final BoundingBox boundingBox) {
		super(key, boundingBox);
		this.nodeId = nodeId;
	}

	public int getNodeId() {
		return nodeId;
	}
	
	/**
	 * Write the node to a stream
	 * @param outputStream
	 * @throws IOException
	 */
	public void writeToStream(final OutputStream outputStream) throws IOException {
		final byte[] keyBytes = getKey().getBytes();

		final byte[] boundingBoxBytes = boundingBox.toByteArray();
		
	    final ByteBuffer nodeIdBytes = DataEncoderHelper.intToByteBuffer(nodeId);
		final ByteBuffer keyLengthBytes = DataEncoderHelper.shortToByteBuffer((short) keyBytes.length);
		final ByteBuffer boxLengthBytes = DataEncoderHelper.intToByteBuffer(boundingBoxBytes.length);
	    
		outputStream.write(nodeIdBytes.array());
		outputStream.write(keyLengthBytes.array());
		outputStream.write(boxLengthBytes.array());
		outputStream.write(keyBytes);
		outputStream.write(boundingBoxBytes);
	}
	
	/**
	 * Read the node from a stream
	 * @param inputStream
	 * @return
	 * @throws IOException 
	 */
	public static RTreeSpatialIndexEntry readFromStream(final InputStream inputStream) throws IOException {
		final byte[] nodeIdBytes = new byte[DataEncoderHelper.INT_BYTES];
		final byte[] keyLengthBytes = new byte[DataEncoderHelper.SHORT_BYTES];
		final byte[] boxLengthBytes = new byte[DataEncoderHelper.INT_BYTES];
		
		ByteStreams.readFully(inputStream, nodeIdBytes, 0, nodeIdBytes.length);
		ByteStreams.readFully(inputStream, keyLengthBytes, 0, keyLengthBytes.length);
		ByteStreams.readFully(inputStream, boxLengthBytes, 0, boxLengthBytes.length);

		final int nodeId = DataEncoderHelper.readIntFromByte(nodeIdBytes);
		final short keyLength = DataEncoderHelper.readShortFromByte(keyLengthBytes);
		final int bboxLength = DataEncoderHelper.readIntFromByte(boxLengthBytes);

		final byte[] keyBytes = new byte[keyLength];
		final byte[] bboxBytes = new byte[bboxLength];
		
		ByteStreams.readFully(inputStream, keyBytes, 0, keyBytes.length);
		ByteStreams.readFully(inputStream, bboxBytes, 0, bboxBytes.length);

		final String key = new String(keyBytes);
		final BoundingBox boundingBox = BoundingBox.fromByteArray(bboxBytes);
		
		return new RTreeSpatialIndexEntry(nodeId, key, boundingBox);
	}

}
