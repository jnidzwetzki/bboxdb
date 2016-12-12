/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package org.bboxdb.storage.entity;

import java.util.Arrays;

public class Tuple implements Comparable<Tuple> {
	
	protected String key;
	protected BoundingBox boundingBox;
	protected byte[] dataBytes;
	protected short seen;
	protected long timestamp;
	
	public Tuple(final String key, final BoundingBox boundingBox, final byte[] dataBytes) {
		super();
		this.key = key;
		this.boundingBox = boundingBox;
		this.dataBytes = dataBytes;
		
		this.timestamp = System.currentTimeMillis();
	}
	
	public Tuple(final String key, final BoundingBox boundingBox, final byte[] dataBytes, long timestamp) {
		super();
		this.key = key;
		this.boundingBox = boundingBox;
		this.dataBytes = dataBytes;
		
		this.timestamp = timestamp;
	}

	/**
	 * Returns the size of the tuple in byte
	 * 
	 * @return
	 */
	public int getSize() {
		int totalSize = 0;
		
		if(dataBytes != null) {
			totalSize += dataBytes.length;
		}
		
		if(boundingBox != null) {
			totalSize += boundingBox.getSize();
		}
		
		return totalSize;
	}

	/**
	 * Get the data of the tuple
	 * 
	 * @return
	 */
	public byte[] getDataBytes() {
		return dataBytes;
	}
	
	/**
	 * Get the timestamp of the tuple
	 * 
	 * @return
	 */
	public long getTimestamp() {
		return timestamp;
	}
	
	/**
	 * Get the key of the tuple
	 * 
	 * @return
	 */
	public String getKey() {
		return key;
	}

	/**
	 * Get the bounding box of the tuple
	 * 
	 * @return
	 */
	public BoundingBox getBoundingBox() {
		return boundingBox;
	}
	
	/**
	 * Get the byte array of the bounding box
	 * @return
	 */
	public byte[] getBoundingBoxBytes() {
		return boundingBox.toByteArray();
	}
	
	@Override
	public String toString() {
		return "Tuple [key=" + key + ", boundingBox=" + boundingBox
				+ ", dataBytes=" + Arrays.toString(dataBytes) + ", seen=" + seen
				+ ", timestamp=" + timestamp + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((boundingBox == null) ? 0 : boundingBox.hashCode());
		result = prime * result + Arrays.hashCode(dataBytes);
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + seen;
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Tuple other = (Tuple) obj;
		if (boundingBox == null) {
			if (other.boundingBox != null)
				return false;
		} else if (!boundingBox.equals(other.boundingBox))
			return false;
		if (!Arrays.equals(dataBytes, other.dataBytes))
			return false;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (seen != other.seen)
			return false;
		if (timestamp != other.timestamp)
			return false;
		return true;
	}

	@Override
	public int compareTo(final Tuple otherTuple) {
		int res = key.compareTo(otherTuple.getKey());
		
		if(res == 0) {
			// The most recent version at top
			return Long.compare(timestamp, otherTuple.getTimestamp()) * -1;
		}
			
		return res;
	}
}