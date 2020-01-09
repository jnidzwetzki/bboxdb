/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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

public class TupleAndTable {

	/**
	 * The tuple
	 */
	private final Tuple tuple;
	
	/**
	 * The table
	 */
	private final String table;
	
	public TupleAndTable(final Tuple tuple, final String table) {
		this.tuple = Objects.requireNonNull(tuple);
		this.table = Objects.requireNonNull(table);
	}

	public Tuple getTuple() {
		return tuple;
	}

	public String getTable() {
		return table;
	}
	
}
