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
package org.bboxdb.storage.sstable;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SStableMetaData;
import org.bboxdb.storage.entity.Tuple;

public class SSTableMetadataBuilder {
	
	/**
	 * The amount of tupes
	 */
	protected long tuples = 0;
	
	/**
	 * The timestamp of the oldest tuple
	 */
	protected long oldestTuple = Long.MAX_VALUE;
	
	/**
	 * The timestamp of the newest tuple
	 */
	protected long newestTuple = Long.MIN_VALUE;

	/**
	 * The coresponding bounding box
	 */
	protected BoundingBox boundingBox;
	
	/**
	 * Update the metadata 
	 */
	public void addTuple(final Tuple tuple) {
		tuples++;
		
		if(boundingBox == null) {
			boundingBox = tuple.getBoundingBox();
		} else {
			// Calculate the bounding box of the current bounding box and
			// the bounding box of the tuple
			boundingBox = BoundingBox.getCoveringBox(boundingBox, tuple.getBoundingBox());
		}
				
		// Update the newest and the oldest tuple
		newestTuple = Math.max(newestTuple, tuple.getTimestamp());
		oldestTuple = Math.min(oldestTuple, tuple.getTimestamp());
	}
	
	/**
	 * Get the metadata object for the seen tuples
	 * @return
	 */
	public SStableMetaData getMetaData() {
		double[] boundingBoxArray = {};
		
		if(boundingBox != null) {
			boundingBoxArray = boundingBox.toDoubleArray();
		}
		
		return new SStableMetaData(tuples, oldestTuple, newestTuple, boundingBoxArray);
	}
}
