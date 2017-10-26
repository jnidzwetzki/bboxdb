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
package org.bboxdb.storage.queryprocessor.queryplan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyQueryPlan implements QueryPlan {

	/**
	 * The key
	 */
	protected final String key;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(KeyQueryPlan.class);

	
	public KeyQueryPlan(final String key) {
		this.key = key;
	}

	@Override
	public Iterator<Tuple> execute(final ReadOnlyTupleStore readOnlyTupleStorage) {
		try {
			final List<Tuple> tupleList = readOnlyTupleStorage.get(key);
			return tupleList.iterator();
		} catch (StorageManagerException e) {
			logger.error("Got exception while preparing iterator", e);
		}
		
		return new ArrayList<Tuple>().iterator();
	}

}
