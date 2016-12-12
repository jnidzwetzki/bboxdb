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
package org.bboxdb.network.packages;

public abstract class NetworkResponsePackage implements NetworkPackage {
	
	/**
	 * The sequence number of the package
	 */
	protected final short sequenceNumber;
	

	public NetworkResponsePackage(final short sequenceNumber) {
		super();
		this.sequenceNumber = sequenceNumber;
	}

	/**
	 * Encode the package
	 * @return 
	 * @throws PackageEncodeError 
	 */
	public abstract byte[] getByteArray() throws PackageEncodeError;

	/**
	 * Get the sequence number of the package
	 * @return
	 */
	public short getSequenceNumber() {
		return sequenceNumber;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + sequenceNumber;
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
		NetworkResponsePackage other = (NetworkResponsePackage) obj;
		if (sequenceNumber != other.sequenceNumber)
			return false;
		return true;
	}
	
}
