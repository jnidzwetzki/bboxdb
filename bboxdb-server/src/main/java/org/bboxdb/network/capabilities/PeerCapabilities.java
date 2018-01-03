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
package org.bboxdb.network.capabilities;

import java.nio.ByteBuffer;

public class PeerCapabilities {

	protected final ByteBuffer capabilities;

	/**
	 * The compression flag
	 */
	public final static short CAPABILITY_COMPRESSION_GZIP = 0;
	
	/**
	 * The readonly flag
	 */
	protected boolean readonly;

	/**
	 * The length of the capabilities
	 */
	public final static short CAPABILITY_BYTES = 4;

	public PeerCapabilities() {
		capabilities = ByteBuffer.allocate(CAPABILITY_BYTES);
		capabilities.clear();
		readonly = false;
	}

	public PeerCapabilities(final byte[] byteArray) {
		if (byteArray.length != CAPABILITY_BYTES) {
			throw new IllegalArgumentException(
					"Wrong lenght of the byteArray: " + byteArray.length);
		}

		capabilities = ByteBuffer.wrap(byteArray);
	}
	
	/**
	 * Freeze the bit set (make read only)
	 */
	public void freeze() {
		readonly = true;
	}

	/**
	 * Is the compression bit set?
	 * 
	 * @return
	 */
	public boolean hasGZipCompression() {
		return getBit(CAPABILITY_COMPRESSION_GZIP);
	}

	/**
	 * Set the compression bit
	 */
	public void setGZipCompression() {
		setBit(CAPABILITY_COMPRESSION_GZIP);
	}

	/**
	 * Clear the compression bit
	 */
	public void clearGZipCompression() {
		clearBit(CAPABILITY_COMPRESSION_GZIP);
	}

	/**
	 * Set the bit
	 * 
	 * @param bit
	 */
	protected void setBit(final int bit) {
		
		if(readonly) {
			throw new IllegalStateException("Unable to set bit in read only mode");
		}
		
		final int byteNumber = bit / 8;
		final int offset = bit % 8;
		byte b = capabilities.get(byteNumber);
		b |= 1 << offset;
		capabilities.put(byteNumber, b);
	}

	/**
	 * Clear the bit
	 * 
	 * @param bit
	 */
	protected void clearBit(final int bit) {
		
		if(readonly) {
			throw new IllegalStateException("Unable to set bit in read only mode");
		}
		
		final int byteNumber = bit / 8;
		final int offset = bit % 8;
		byte b = capabilities.get(byteNumber);
		b &= ~(1 << offset);
		capabilities.put(byteNumber, b);
	}

	/**
	 * Is the bit set?
	 * 
	 * @param bit
	 * @return
	 */
	protected boolean getBit(final int bit) {
		final int byteNumber = bit / 8;
		final int offset = bit % 8;
		byte b = capabilities.get(byteNumber);
		b &= 1 << offset;

		return b != 0;
	}

	/**
	 * Convert the capabilities into a byte array
	 * 
	 * @return
	 */
	public byte[] toByteArray() {
		return capabilities.array();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((capabilities == null) ? 0 : capabilities.hashCode());
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
		PeerCapabilities other = (PeerCapabilities) obj;
		if (capabilities == null) {
			if (other.capabilities != null)
				return false;
		} else if (!capabilities.equals(other.capabilities))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "PeerCapabilities [capabilities=" + capabilities + "]";
	}

}
