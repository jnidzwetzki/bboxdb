/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb.storage.tuplestore;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;

public interface ReadWriteTupleStore extends ReadOnlyTupleStore {

	/**
	 * Store a tuple
	 * @param tuple
	 * @throws StorageManagerException
	 */
	public void put(final Tuple tuple) throws StorageManagerException;

	/**
	 * Delete a tuple
	 * @param key
	 * @param timestamp
	 * @throws StorageManagerException
	 */
	public void delete(final String key, final long timestamp) throws StorageManagerException;
	
	/**
	 * Truncate the stored data
	 * @throws StorageManagerException
	 */
	public void clear() throws StorageManagerException;
	
}