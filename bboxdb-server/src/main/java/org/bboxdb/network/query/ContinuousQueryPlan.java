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
package org.bboxdb.network.query;

import java.util.List;

import org.bboxdb.network.query.transformation.TupleTransformation;

public abstract class ContinuousQueryPlan {

	/**
	 * The stream table
	 */
	protected final String streamTable;
	
	/**
	 * The transformations of the stream entry
	 */
	protected final List<TupleTransformation> streamTransformation;
	
	/**
	 * The table name to query
	 */
	protected final String tableName;
	
	/**
	 * Report positive or negative elements to the user
	 */
	protected final boolean reportPositiveNegative;

	public ContinuousQueryPlan(final String streamTable, final List<TupleTransformation> streamTransformation, 
			final String tableName, final boolean reportPositiveNegative) {
		this.streamTable = streamTable;
		this.streamTransformation = streamTransformation;
		this.tableName = tableName;
		this.reportPositiveNegative = reportPositiveNegative;
	}	
	
}
