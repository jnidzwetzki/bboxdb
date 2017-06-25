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
package org.bboxdb.storage.sstable.spatialindex.rtree;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map.Entry;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.sstable.spatialindex.BoundingBoxEntity;

public class QuadraticSeedPicker<T extends BoundingBoxEntity> {

	/**
	 * Find the seeds for the split
	 * @param insertedNode
	 * @return
	 */
	protected Entry<T, T> quadraticPickSeeds(final List<T> allEntries) {
		
		assert(allEntries.size() >= 2);
		
		double maxWaste = Double.MAX_VALUE;
		Entry<T, T> result = null;
		
		for(final T box1 : allEntries) {
			for(final T box2 : allEntries) {
				
				if(box1 == box2) {
					continue;
				}
				
				final BoundingBox boundingBox1 = box1.getBoundingBox();
				final BoundingBox boundingBox2 = box2.getBoundingBox();
				
				final double coveringArea 
					= BoundingBox.getCoveringBox(boundingBox1, boundingBox2).getVolume();
				
				final double waste = coveringArea - boundingBox1.getVolume()
						- boundingBox2.getVolume();
				
				if(waste < maxWaste) {
					result = new AbstractMap.SimpleImmutableEntry<T,T>(box1, box2);
					maxWaste = waste;
				}
			}	
		}
		
		assert(result != null) : "Unable to find seeds";
		
		// Remove seeds from available objects
		allEntries.remove(result.getKey());
		allEntries.remove(result.getValue());
		
		return result;
	}
}
