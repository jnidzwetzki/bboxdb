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
package org.bboxdb.storage.queryprocessor.operator;

import java.util.Iterator;

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

	public SpatialIndexReadOperator(final TupleStoreManager tupleStoreManager) {
		this(tupleStoreManager, Hyperrectangle.FULL_SPACE);
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
	
	/**
	 * Get the bounding box of the operation
	 * @return
	 */
	public Hyperrectangle getBoundingBox() {
		return boundingBox;
	}
}
