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
package org.bboxdb.tools.converter.osm.util;

import java.nio.ByteBuffer;

import org.bboxdb.misc.Const;
import org.bboxdb.util.DataEncoderHelper;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;

public class SerializableNode {

	/**
	 * The id
	 */
	protected final long id;
	
	/**
	 * The latitude
	 */
	protected final double latitude;
	
	/**
	 * The longitude
	 */
	protected final double longitude;

	/**
	 * The timestamp
	 */
	protected final long timestamp;

	/**
	 * The version
	 */
	protected final int version;
	
	public SerializableNode(final Node node) {
		this.latitude = node.getLatitude();
		this.longitude = node.getLongitude();
		this.id = node.getId();
		this.timestamp = node.getTimestamp().getTime();
		this.version = node.getVersion();
	}

	public SerializableNode(final long id, final double latitude, 
			final double longitude, final long timestamp, final int version) {
		this.id = id;
		this.latitude = latitude;
		this.longitude = longitude;
		this.timestamp = timestamp;
		this.version = version;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public long getId() {
		return id;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getVersion() {
		return version;
	}
	
	/**
	 * Convert to byte array
	 * @return
	 */
	public byte[] toByteArray() {
		// 2x long, 2x double, 1x int
		
		final int length = (2 * DataEncoderHelper.LONG_BYTES) + (2 * DataEncoderHelper.DOUBLE_BYTES)
				+ DataEncoderHelper.INT_BYTES;

		final ByteBuffer bb = ByteBuffer.allocate(length);
		bb.order(Const.APPLICATION_BYTE_ORDER);
		bb.putLong(id);
		bb.putDouble(latitude);
		bb.putDouble(longitude);
		bb.putLong(timestamp);
		bb.putInt(version);

		return bb.array();
	}
	
	/**
	 * Read from byte array
	 * @param bytes
	 * @return
	 */
	public static SerializableNode fromByteArray(final byte[] bytes) {
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		bb.order(Const.APPLICATION_BYTE_ORDER);
		final long id = bb.getLong();
		final double latitude = bb.getDouble();
		final double longitude = bb.getDouble();
		final long timestamp = bb.getLong();
		final int version = bb.getInt();
		
		assert (! bb.hasRemaining()) : "Byte buffer is not empty";
		
		return new SerializableNode(id, latitude, longitude, timestamp, version);
	}

}
