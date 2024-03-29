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
package org.bboxdb.query.queryprocessor.operator.join;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.query.queryprocessor.operator.SpatialIndexReadOperator;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;

public class SpatialIterator implements Iterator<MultiTuple> {

	/**
	 * The stream source
	 */
	private final Iterator<MultiTuple> tupleStreamSource;

	/**
	 * The index reader
	 */
	private final SpatialIndexReadOperator indexReader;

	/**
	 * The tuple stream source
	 */
	private MultiTuple tupleFromStreamSource = null;

	/**
	 * The candidates
	 */
	private Iterator<MultiTuple> candidatesForCurrentTuple = null;

	/**
	 * The next tuple 
	 */
	private MultiTuple nextTuple = null;

	/**
	 * The current operation range
	 */
	private Hyperrectangle currentOperationRange = null;

	/**
	 * The query box
	 */
	private final Hyperrectangle queryBox;

	public SpatialIterator(final Iterator<MultiTuple> tupleStreamSource, 
			final SpatialIndexReadOperator indexReader) {

		this.tupleStreamSource = tupleStreamSource;
		this.indexReader = indexReader;
		this.queryBox = indexReader.getBoundingBox();
	}


	@Override
	public boolean hasNext() {

		while(nextTuple == null) {					
			// Join partner exhausted, try next tuple
			while(candidatesForCurrentTuple == null || ! candidatesForCurrentTuple.hasNext()) {						
				// Fetch next tuple from stream source
				if(! tupleStreamSource.hasNext()) { 
					return false;
				} else {
					// Start a new index scan for the next steam source tuple bounding box
					tupleFromStreamSource = tupleStreamSource.next();
					CloseableHelper.closeWithoutException(indexReader);
					final Hyperrectangle bbox = tupleFromStreamSource.getBoundingBox();

					// Limit the scan operation to the intersection of the query range and the tuple from 
					// stream. Otherwise intersections in other areas are detected.
					if(queryBox != Hyperrectangle.FULL_SPACE) {
						currentOperationRange = bbox.getIntersection(queryBox);
					} else {
						currentOperationRange = bbox;
					}
					
					//logger.info("----> Operation box: " + currentOperationRange.toCompactString() 
					//	+ "(bbox: " + bbox.toCompactString() + " / " + queryBox.toCompactString() + ")");
					
					indexReader.setBoundingBox(currentOperationRange);
					candidatesForCurrentTuple = indexReader.iterator();							
				} 
			}

			final MultiTuple nextJoinedTuple = candidatesForCurrentTuple.next();
			final Tuple nextCandidateTuple = nextJoinedTuple.convertToSingleTupleIfPossible();
			
			assert (nextCandidateTuple.getBoundingBox().intersects(currentOperationRange)) : "Wrong join, no overlap";
			nextTuple = buildNextJoinedTuple(nextCandidateTuple);
		}

		return nextTuple != null;
	}

	/**
	 * Build the next joined tuple
	 * @param nextCandidateTuple
	 * @return
	 */
	protected MultiTuple buildNextJoinedTuple(final Tuple nextCandidateTuple) {

		// Build tuple store name
		final List<String> tupleStoreNames = new ArrayList<>();
		tupleStoreNames.addAll(tupleFromStreamSource.getTupleStoreNames());
		tupleStoreNames.add(indexReader.getTupleStoreName().getFullnameWithoutPrefix());

		// Build tuple
		final List<Tuple> tupesToJoin = new ArrayList<>();		
		tupesToJoin.addAll(tupleFromStreamSource.getTuples());
		tupesToJoin.add(nextCandidateTuple);

		// Filter deleted tuples
		for(final Tuple tuple : tupesToJoin) {
			if(tuple instanceof DeletedTuple) {
				return null;
			}
		}

		return new MultiTuple(tupesToJoin, tupleStoreNames);
	}

	@Override
	public MultiTuple next() {

		if(nextTuple == null) {
			throw new IllegalArgumentException("Next tuple is null, do you forget to call hasNext()?");
		}

		final MultiTuple returnTuple = nextTuple;
		nextTuple = null;
		return returnTuple;
	}
}