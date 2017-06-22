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
package org.bboxdb.storage.sstable.spatialindex;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.util.io.DataEncoderHelper;

import com.google.common.io.ByteStreams;

public class SpatialIndexEntry implements BoundingBoxEntity {

	/**
	 * The key
	 */
	protected final String key;
	
	/**
	 * The bounding box
	 */
	protected final BoundingBox boundingBox;

	public SpatialIndexEntry(final String key, final BoundingBox boundingBox) {
		this.key = key;
		this.boundingBox = boundingBox;
	}
	
	/**
	 * Get the key
	 * @return
	 */
	public String getKey() {
		return key;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.storage.sstable.spatialindex.BoundingBoxEntity#getBoundingBox()
	 */
	@Override
	public BoundingBox getBoundingBox() {
		return boundingBox;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((boundingBox == null) ? 0 : boundingBox.hashCode());
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SpatialIndexEntry other = (SpatialIndexEntry) obj;
		if (boundingBox == null) {
			if (other.boundingBox != null)
				return false;
		} else if (!boundingBox.equals(other.boundingBox))
			return false;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SpatialIndexEntry [key=" + key + ", boundingBox=" + boundingBox + "]";
	}
	
	/**
	 * Write the node to a stream
	 * @param outputStream
	 * @throws IOException
	 */
	public void writeToStream(final OutputStream outputStream) throws IOException {
		final byte[] keyBytes = getKey().getBytes();

		final byte[] boundingBoxBytes = boundingBox.toByteArray();
		
		final ByteBuffer keyLengthBytes = DataEncoderHelper.shortToByteBuffer((short) keyBytes.length);
		final ByteBuffer boxLengthBytes = DataEncoderHelper.intToByteBuffer(boundingBoxBytes.length);
	    
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
	public static SpatialIndexEntry readFromStream(final InputStream inputStream) throws IOException {
		final byte[] keyLengthBytes = new byte[DataEncoderHelper.SHORT_BYTES];
		final byte[] boxLengthBytes = new byte[DataEncoderHelper.INT_BYTES];
		
		ByteStreams.readFully(inputStream, keyLengthBytes, 0, keyLengthBytes.length);
		ByteStreams.readFully(inputStream, boxLengthBytes, 0, boxLengthBytes.length);

		final short keyLength = DataEncoderHelper.readShortFromByte(keyLengthBytes);
		final int bboxLength = DataEncoderHelper.readIntFromByte(boxLengthBytes);

		final byte[] keyBytes = new byte[keyLength];
		final byte[] bboxBytes = new byte[bboxLength];
		
		ByteStreams.readFully(inputStream, keyBytes, 0, keyBytes.length);
		ByteStreams.readFully(inputStream, bboxBytes, 0, bboxBytes.length);

		final String key = new String(keyBytes);
		final BoundingBox boundingBox = BoundingBox.fromByteArray(bboxBytes);
	
		return new SpatialIndexEntry(key, boundingBox);
	}

}
