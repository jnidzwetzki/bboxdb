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
package org.bboxdb.distribution.partitioner.regionsplit.tuplesink;

import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;

public class LocalTupleSink extends AbstractTupleSink {
	/**
	 * The storage manager to store the tuple
	 */
	protected final TupleStoreManager storageManager;
	
	public LocalTupleSink(final TupleStoreName tablename, final TupleStoreManager storageManager) {
		super(tablename);
		this.storageManager = storageManager;
	}

	@Override
	public void sinkTuple(final Tuple tuple) throws Exception {
		sinkedTuples++;
		storageManager.put(tuple);
	}
}
