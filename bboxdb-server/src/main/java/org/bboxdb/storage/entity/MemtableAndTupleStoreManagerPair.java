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
package org.bboxdb.storage.entity;

import java.util.Objects;

import org.bboxdb.storage.memtable.Memtable;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;

public class MemtableAndTupleStoreManagerPair {

	/**
	 * The memtale
	 */
	protected final Memtable memtable;
	
	/**
	 * The sstable manager
	 */
	protected final TupleStoreManager tupleStoreManager;

	public MemtableAndTupleStoreManagerPair(final Memtable memtable, final TupleStoreManager tupleStoreManager) {
		this.memtable = Objects.requireNonNull(memtable);
		this.tupleStoreManager = Objects.requireNonNull(tupleStoreManager);
	}
	
	/**
	 * Get the memtable
	 * @return
	 */
	public Memtable getMemtable() {
		return memtable;
	}
	
	/**
	 * Get the tuple store manager
	 * @return
	 */
	public TupleStoreManager getTupleStoreManager() {
		return tupleStoreManager;
	}
}
