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

import java.util.List;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.sstable.spatialindex.BoundingBoxEntity;

public class QuadraticSeedPicker<T extends BoundingBoxEntity> {

	/**
	 * Find the seeds for the split
	 * @param insertedNode
	 * @return
	 */
	protected void quadraticPickSeeds(final List<T> allEntries, 
			final List<T> seeds) {
		
		assert(seeds.isEmpty());
		assert(allEntries.size() >= 2);
		
		double maxWaste = Double.MAX_VALUE;
		
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
					seeds.clear();
					seeds.add(box1);
					seeds.add(box2);
					maxWaste = waste;
				}
			}	
		}
		
		assert(seeds.size() == 2) : "Number of seeds don't match: " + seeds.size();
		
		// Remove seeds from available objects
		allEntries.removeAll(seeds);
	}
}
