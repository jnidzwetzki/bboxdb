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
package org.bboxdb.storage.entity;

import java.util.Arrays;
import java.util.Objects;

import org.bboxdb.commons.MicroSecondTimestampProvider;
import org.bboxdb.storage.util.TupleHelper;

public class Tuple implements Comparable<Tuple>, PagedTransferableEntity {
	
	/**
	 * The key of the tuple
	 */
	protected String key;
	
	/**
	 * The bounding box of the tuple
	 */
	protected BoundingBox boundingBox;
	
	/**
	 * The data of the tuple
	 */
	protected byte[] dataBytes;
	
	/**
	 * The version timestamp
	 */
	protected final long versionTimestamp;
	
	/**
	 * The received timestamp
	 */
	protected final long receivedTimestamp;
	
	public Tuple(final String key, final BoundingBox boundingBox, final byte[] dataBytes) {
		this.key = Objects.requireNonNull(key);
		this.boundingBox = boundingBox;
		this.dataBytes = Objects.requireNonNull(dataBytes);
		this.versionTimestamp = MicroSecondTimestampProvider.getNewTimestamp();
		this.receivedTimestamp = MicroSecondTimestampProvider.getNewTimestamp();
	}
	
	public Tuple(final String key, final BoundingBox boundingBox, 
			final byte[] dataBytes, final long versionTimestamp) {
		
		this.key = Objects.requireNonNull(key);
		this.boundingBox = boundingBox;
		this.dataBytes = Objects.requireNonNull(dataBytes);
		this.versionTimestamp = versionTimestamp;
		this.receivedTimestamp = System.currentTimeMillis();
	}
	
	public Tuple(final String key, final BoundingBox boundingBox, 
			final byte[] dataBytes, final long versionTimestamp,
			final long receivedTimestamp) {
		
		this.key = key;
		this.boundingBox = boundingBox;
		this.dataBytes = dataBytes;
		this.versionTimestamp = versionTimestamp;
		this.receivedTimestamp = receivedTimestamp;
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
	 * Get the version timestamp of the tuple
	 * 
	 * @return
	 */
	public long getVersionTimestamp() {
		return versionTimestamp;
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
		return "Tuple [key=" + key + ", boundingBox=" + boundingBox + ", dataBytes=" + Arrays.toString(dataBytes)
			    + ", versionTimestamp=" + versionTimestamp + ", receivedTimestamp="
				+ receivedTimestamp + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((boundingBox == null) ? 0 : boundingBox.hashCode());
		result = prime * result + Arrays.hashCode(dataBytes);
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + (int) (versionTimestamp ^ (versionTimestamp >>> 32));
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
		if (versionTimestamp != other.versionTimestamp)
			return false;
		return true;
	}

	@Override
	public int compareTo(final Tuple otherTuple) {
		int res = key.compareTo(otherTuple.getKey());
		
		if(res == 0) {
			// The most recent version at top
			return Long.compare(versionTimestamp, otherTuple.getVersionTimestamp()) * -1;
		}
			
		return res;
	}
	
	/**
	 * Get the received timestamp
	 * @return
	 */
	public long getReceivedTimestamp() {
		return receivedTimestamp;
	}

	/**
	 * Get the formated string
	 * @return
	 */
	public String getFormatedString() {
		if(TupleHelper.isDeletedTuple(this)) {
			return String.format("Key %s, DELETED, version timestamp=%d\n", 
					getKey(), getVersionTimestamp());
		} else {
			return String.format("Key %s, BoundingBox=%s, value=%s, version timestamp=%d\n",
					getKey(), getBoundingBox().toCompactString(), 
					new String(getDataBytes()), getVersionTimestamp());
		}
	}

	@Override
	public EntityIdentifier getEntityIdentifier() {
		return new TupleEntityIdentifier(key, versionTimestamp);
	}
	
}