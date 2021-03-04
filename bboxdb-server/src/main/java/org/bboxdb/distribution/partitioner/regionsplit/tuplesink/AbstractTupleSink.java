/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.distribution.partitioner.regionsplit.tuplesink;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;

public abstract class AbstractTupleSink {
	
	/**
	 * The table name for the sink
	 */
	protected final String tablename;
	
	/**
	 * The amount of sinked tuples
	 */
	public long sinkedTuples;
	
	public AbstractTupleSink(final TupleStoreName tablename) {
		this.tablename = tablename.getFullname();
		this.sinkedTuples = 0;
	}
	
	/**
	 * Get the amount of sinked tuples
	 * @return
	 */
	public long getSinkedTuples() {
		return sinkedTuples;
	}
	
	/**
	 * Process the given tuple
	 * @param tuple
	 * @throws StorageManagerException
	 */
	public abstract void sinkTuple(final Tuple tuple) throws StorageManagerException;
}