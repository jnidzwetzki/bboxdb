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
package org.bboxdb.distribution.partitioner;


public enum DistributionRegionState {
	
	CREATING("creating"),
	ACTIVE("active"),
	ACTIVE_FULL("active-full"),
	SPLITTING("splitting"),
	SPLIT("split"),
	SPLIT_MERGING("split-merging"),
	MERGING("merging");

	/**
	 * The string representation
	 */
	protected final String stringValue;
		
	private DistributionRegionState(final String stringValue) {
		this.stringValue = stringValue;
	}
	
	/**
	 * Get the string representation
	 * @return
	 */
	public String getStringValue() {
		return stringValue;
	}
	
	/**
	 * Convert the string value into an enum
	 * @param stringValue
	 * @return
	 */
	public static DistributionRegionState fromString(final String stringValue) {
		if (stringValue == null) {
			throw new RuntimeException("stringValue is null");
		}
    
		for(final DistributionRegionState nodeState : DistributionRegionState.values()) {
			if(stringValue.equals(nodeState.getStringValue())) {
				return nodeState;
			}
		}

		throw new RuntimeException("Unable to convert " + stringValue + " into enum");
	}
}
