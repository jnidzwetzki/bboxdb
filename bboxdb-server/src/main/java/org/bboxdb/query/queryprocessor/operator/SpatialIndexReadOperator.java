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
package org.bboxdb.query.queryprocessor.operator;

import java.util.Iterator;
import java.util.List;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;

public class SpatialIndexReadOperator extends AbstractTablescanOperator {

	/**
	 * The bounding box
	 */
	private Hyperrectangle boundingBox;

	public SpatialIndexReadOperator(final TupleStoreManager tupleStoreManager,
			final Hyperrectangle boundingBox) {

		super(tupleStoreManager);
		this.boundingBox = boundingBox;
	}

	/**
	 * Set a new bounding box to read
	 * @param boundingBox
	 */
	public void setBoundingBox(final Hyperrectangle boundingBox) {
		this.boundingBox = boundingBox;
	}

	@Override
	protected Iterator<Tuple> setupNewTuplestore(final ReadOnlyTupleStore nextStorage) {
		return nextStorage.getAllTuplesInBoundingBox(boundingBox);
	}

	@Override
	protected void filterTupleVersions(final List<Tuple> tupleVersions) {
		
		/**
		 * We read the tuples from the index. Than the newest tuple for
		 * the key is read from storage. This new tuple can have a bounding 
		 * box outside of our search range. These tuples needs to be removed.
		 */
		tupleVersions.removeIf(t -> isNotCovered(t));
	}

	/**
	 * Build the deletion predicate
	 *
	 * @return
	 */
	private boolean isNotCovered(final Tuple tuple) {
		
		if(tuple.getBoundingBox() == null) {
			return false;
		}

		return ! tuple.getBoundingBox().intersects(boundingBox);
	}
	
	/**
	 * Get the bounding box of the operation
	 * @return
	 */
	public Hyperrectangle getBoundingBox() {
		return boundingBox;
	}
}
