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
package org.bboxdb.storage.sstable.spatialindex;

import org.bboxdb.storage.entity.BoundingBox;

public class SpatialIndexEntry implements BoundingBoxEntity {

	/**
	 * The key
	 */
	protected final String key;
	
	/**
	 * The bounding box
	 */
	protected final BoundingBox boundingBox;

	public SpatialIndexEntry(final String key, final BoundingBox boundingBox) {
		this.key = key;
		this.boundingBox = boundingBox;
	}
	
	/**
	 * Get the key
	 * @return
	 */
	public String getKey() {
		return key;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.storage.sstable.spatialindex.BoundingBoxEntity#getBoundingBox()
	 */
	@Override
	public BoundingBox getBoundingBox() {
		return boundingBox;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((boundingBox == null) ? 0 : boundingBox.hashCode());
		result = prime * result + ((key == null) ? 0 : key.hashCode());
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
		SpatialIndexEntry other = (SpatialIndexEntry) obj;
		if (boundingBox == null) {
			if (other.boundingBox != null)
				return false;
		} else if (!boundingBox.equals(other.boundingBox))
			return false;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}
	
}
