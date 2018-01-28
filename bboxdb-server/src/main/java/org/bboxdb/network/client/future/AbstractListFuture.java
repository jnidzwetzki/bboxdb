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
package org.bboxdb.network.client.future;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bboxdb.network.client.BBoxDBClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractListFuture<T> extends OperationFutureImpl<List<T>> implements Iterable<T> {

	/**
	 * Is the result complete or only a page?
	 */
	protected final Map<Integer, Boolean> resultComplete = new HashMap<>();
	
	/**
	 * The connections for the paging
	 */
	protected final Map<Integer, BBoxDBClient> connections = new HashMap<>();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(AbstractListFuture.class);

	public AbstractListFuture() {
		super();
	}

	public AbstractListFuture(final int numberOfFutures) {
		super(numberOfFutures);
	}
	

	/**
	 * Check whether the result is only a page or complete
	 * 
	 * @param resultId
	 * @return
	 */
	public boolean isCompleteResult(final int resultId) {
		checkFutureSize(resultId);
		
		if(! resultComplete.containsKey(resultId)) {
			return false;
		}

		return resultComplete.get(resultId);
	}

	/**
	 * Set the completed flag for a result
	 * 
	 * @param resultId
	 * @param completeResult
	 */
	public void setCompleteResult(final int resultId, final boolean completeResult) {
		checkFutureSize(resultId);

		resultComplete.put(resultId, completeResult);
	}
	
	/**
	 * Set the BBoxDB connection for paging
	 * @param resultId
	 * @param bboxdbClient
	 */
	public void setConnectionForResult(final int resultId, final BBoxDBClient bboxdbClient) {
		checkFutureSize(resultId);

		connections.put(resultId, bboxdbClient);
	}
	
	/**
	 * Get the bboxdbClient for the given resultId (needed to request next pages)
	 * @param resultId
	 * @return
	 */
	public BBoxDBClient getConnectionForResult(final int resultId) {
		checkFutureSize(resultId);

		if(! connections.containsKey(resultId)) {
			logger.error("getConnectionForResult() called with id {}, but connection is unknown", resultId);
			return null;
		}
		
		return connections.get(resultId);
	}

	/**
	 * Get a list with all results
	 * @return
	 */
	protected List<T> getListWithAllResults() {
		final List<T> allTuples = new ArrayList<>();
		
		for(int i = 0; i < getNumberOfResultObjets(); i++) {
			try {
				final List<T> tupleResult = get(i);
				
				if(tupleResult != null) {
					for(final T tuple : tupleResult) {
						allTuples.add(tuple);
					}
				}
				
			} catch (Exception e) {
				logger.error("Got exception while iterating", e);
			}
		}
		return allTuples;
	}
	
	/**
	 * Return a iterator for all tuples
	 * @return
	 */
	@Override
	public Iterator<T> iterator() {
		if(! isDone() ) {
			throw new IllegalStateException("Future is not done, unable to build iterator");
		}
		
		if( isFailed() ) {
			throw new IllegalStateException("The future has failed, unable to build iterator");
		}
		
		// Is at least result paged? So, we use the threaded iterator 
		// that requests more tuples/pages in the background
		final boolean pagedResult = resultComplete.values().stream().anyMatch(e -> e == false);
		
		if(pagedResult) {
			return createThreadedIterator();
		} else {
			return createSimpleIterator();
		}	
	}
	
	/**
	 * Create the threaded iterator
	 */
	protected abstract Iterator<T> createThreadedIterator();

	/**
	 * Create the simple iterator
	 */
	protected abstract Iterator<T> createSimpleIterator();

}
