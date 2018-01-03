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

public enum UpdateAnomalyResolver {

	NONE((byte) 0),
	RESOLVE_ON_READ((byte) 1),
	RESOLVE_ON_WRITE((byte) 2);
	
	protected final byte value;

	private UpdateAnomalyResolver(final byte value) {
		this.value = value;
	}

	public byte getValue() {
		return value;
	}
	

	/**
	 * Construct update anomaly resolver from byte
	 * @param updateAnomalyResolver
	 * @param tupleStoreConfiguration
	 * @return
	 */
	public static UpdateAnomalyResolver buildFromByte(final byte updateAnomalyResolver) {
		
		if(updateAnomalyResolver == (byte) 0) {
			return UpdateAnomalyResolver.NONE;
		} else if(updateAnomalyResolver == (byte) 1) {
			return UpdateAnomalyResolver.RESOLVE_ON_READ;
		} else if(updateAnomalyResolver == (byte) 2) {
			return UpdateAnomalyResolver.RESOLVE_ON_WRITE;
		} else {
			throw new IllegalArgumentException("Illegal update anomaly resolver: " + updateAnomalyResolver);
		}
	}

}
