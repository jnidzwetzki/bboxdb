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
package org.bboxdb.storage.sstable;

public enum SSTableCreator {
	
	UNKNOWN("unkown"), 
	MEMTABLE("memtable"), 
	MINOR("minor"), 
	MAJOR("major");

	private final String creator;

	SSTableCreator(final String creator) {
		this.creator = creator;
	}

	public String getCreatorString() {
		return creator;
	}

	public static SSTableCreator fromString(final String value) {
		for (SSTableCreator orderType : SSTableCreator.values()) {
			if (orderType.getCreatorString().equalsIgnoreCase(value)) {
				return orderType;
			}
		}

		return SSTableCreator.UNKNOWN;
	}
}
