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
package org.bboxdb.storage.sstable.spatialindex;

import java.io.RandomAccessFile;
import java.util.List;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.storage.StorageManagerException;


public interface SpatialIndexBuilder {
	
	/**
	 * Construct the index from a list of tuples
	 * 
	 * @param tuples
	 * @return 
	 */
	public boolean bulkInsert(final List<SpatialIndexEntry> elements);
	
	/**
	 * Insert one element into the index
	 * 
	 * @param element
	 * @return 
	 */
	public boolean insert(final SpatialIndexEntry element);

	/**
	 * Read the index from a data stream
	 * 
	 * @param outputStream
	 * @throws StorageManagerException 
	 */
	public void writeToFile(final RandomAccessFile randomAccessFile) throws StorageManagerException;
	
	/**
	 * Find the entries for the given region
	 * @param boundingBox
	 * @return
	 */
	public List<? extends SpatialIndexEntry> getEntriesForRegion(final BoundingBox boundingBox);

}
