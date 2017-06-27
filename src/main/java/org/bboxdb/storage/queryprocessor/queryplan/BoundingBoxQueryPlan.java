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

import java.util.Iterator;

import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.datasource.DataSource;
import org.bboxdb.storage.queryprocessor.datasource.SpatialIndexDataSource;

public class BoundingBoxQueryPlan implements QueryPlan {

	/**
	 * The bounding box for the query
	 */
	protected final BoundingBox boundingBox;
	
	public BoundingBoxQueryPlan(final BoundingBox boundingBox) {
		this.boundingBox = boundingBox;
	}

	@Override
	public Iterator<Tuple> execute(final ReadOnlyTupleStorage readOnlyTupleStorage) {
		final DataSource dataSource = new SpatialIndexDataSource(readOnlyTupleStorage, boundingBox);
		
		return dataSource.iterator();
	}

}
