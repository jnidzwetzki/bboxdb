/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.network.client.future;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.network.client.ScalephantClient;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;

public class TupleListFuture extends OperationFuture<List<Tuple>> {
	
	/**
	 * Is the result complete or only a page?
	 */
	protected final Map<Integer, Boolean> resultComplete = new HashMap<>();
	
	/**
	 * The connections for the paging
	 */
	protected final Map<Integer, ScalephantClient> connections = new HashMap<>();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleListFuture.class);

	public TupleListFuture() {
		super();
	}

	public TupleListFuture(final int numberOfFutures) {
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
		
		if(! resultComplete.containsKey(resultComplete)) {
			return false;
		}

		return resultComplete.get(resultComplete);
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
	 * Set the scalephant connection for pagig
	 * @param resultId
	 * @param scalephantClient
	 */
	public void setConnectionForResult(final int resultId, final ScalephantClient scalephantClient) {
		checkFutureSize(resultId);

		connections.put(resultId, scalephantClient);
	}
	
	public ScalephantClient getConnectionForResult(final int resultId) {
		checkFutureSize(resultId);

		if(! connections.containsKey(resultId)) {
			logger.error("getConnectionForResult() called with id {}, but connection is unknown", resultId);
			return null;
		}
		
		return connections.get(resultId);
	}
	
}
