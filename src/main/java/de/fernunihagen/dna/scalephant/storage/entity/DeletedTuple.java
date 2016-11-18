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
package de.fernunihagen.dna.scalephant.storage.entity;

import de.fernunihagen.dna.scalephant.storage.sstable.SSTableConst;

public class DeletedTuple extends Tuple {

	public DeletedTuple(final String key) {
		super(key, null, null);
	}

	@Override
	public String toString() {
		return "DeletedTuple [key=" + key + ", timestamp=" + timestamp + "]";
	}
	
	@Override
	public byte[] getDataBytes() {
		return SSTableConst.DELETED_MARKER;
	}
	
	@Override
	public byte[] getBoundingBoxBytes() {
		return SSTableConst.DELETED_MARKER;
	}
}
