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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.bboxdb.storage.entity.BoundingBox;


public interface SpatialIndexStrategy {
	
	/**
	 * Construct the index from a list of tuples
	 * 
	 * @param tuples
	 */
	public void insertTuples(final Map<String, BoundingBox> elements);
	
	/**
	 * Persist the index 
	 * 
	 * @param inputStream
	 */
	public void readFromStream(final InputStream inputStream);
	
	/**
	 * Read the index from a data stream
	 * 
	 * @param outputStream
	 */
	public void writeToStream(final OutputStream outputStream);
	
	/**
	 * Find the keys for the given region
	 * @param boundingBox
	 * @return
	 */
	public List<String> getKeysForRegion(final BoundingBox boundingBox);

}
