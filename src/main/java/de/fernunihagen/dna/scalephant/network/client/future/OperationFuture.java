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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class OperationFuture {

	public final List<FutureImplementation> futures;
	
	public OperationFuture() {
		this(0);
	}
	
	public OperationFuture(final int numberOfFutures) {
		futures = new ArrayList<FutureImplementation>(numberOfFutures);
		
		for(int i = 0; i < numberOfFutures; i++) {
			futures.add(new FutureImplementation());
		}
	}
	
	/**
	 * Set the request id of the operation
	 * @return
	 */
	public void setRequestId(final int resultId, final short requestId) {
		checkFutureSize(resultId);
		
		futures.get(resultId).setRequestId(requestId);
	}

	/**
	 * Get the request id of the operation
	 * @return
	 */
	public short getRequestId(final int resultId) {
		checkFutureSize(resultId);
		
		return futures.get(resultId).getRequestId();
	}

	/**
	 * Set the result of the operation
	 * @param result
	 */
	public void setOperationResult(final int resultId, final Object result) {
		checkFutureSize(resultId);
		
		futures.get(resultId).setOperationResult(result);
	}
	
	/**
	 * Set the additional message
	 * @param resultId
	 * @param message
	 */
	public void setMessage(final int resultId, final String message) {
		checkFutureSize(resultId);
		
		futures.get(resultId).setMessage(message);
	}

	/**
	 * Get the additional message
	 * @param resultId
	 * @return 
	 */
	public String getMessage(final int resultId) {
		checkFutureSize(resultId);
		
		return futures.get(resultId).getMessage();
	}

	/**
	 * Throw an exception when the result id is unknown
	 * @param resultId
	 */
	protected void checkFutureSize(final int resultId) {
		if(resultId > futures.size()) {
			throw new IllegalArgumentException("Unable to access future with id: " + resultId + "(total " + futures.size() + ")");
		}
	}
	
	/**
	 * Is the future processed successfully or are errors occurred?
	 * @return
	 */
	public boolean isFailed() {
		
		for(final FutureImplementation future : futures) {
			if(future.isFailed()) {
				return true;
			}
		}
		
		return false;
	}

	/**
	 * Set the failed state
	 */
	public void setFailedState() {
		for(final FutureImplementation future : futures) {
			future.setFailedState();
		}
	}

	/**
	 * Is the future done
	 */
	public boolean isDone() {
		for(final FutureImplementation future : futures) {
			if(! future.isDone()) {
				return false;
			}
		}
		
		return true;
	}

	/**
	 * Get the number of result objects
	 * @return
	 */
	public int getNumberOfResultObjets() {
		return futures.size();
	}

	/**
	 * Get the result of the future
	 * @return
	 */
	public Object get(final int resultId) throws InterruptedException, ExecutionException {
		
		checkFutureSize(resultId);
		
		return futures.get(resultId).get();
	}

    /**
	 * Get the result of the future
	 * @return
     * @throws TimeoutException 
	 */
	public Object get(final int resultId, final long timeout, final TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		
		checkFutureSize(resultId);
		
		return futures.get(resultId).get(timeout, unit);
	}
	
    /**
     * Wait for all futures to complete
     * @return
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
	public boolean waitForAll() throws InterruptedException, ExecutionException {
		for(int i = 0; i < getNumberOfResultObjets(); i++) {
			get(i);
		}
		
		return true;
	}

	/**
	 * Fire the completion event
	 */
	public void fireCompleteEvent() {
		for(final FutureImplementation future : futures) {
			future.fireCompleteEvent();
		}
	}

	/**
	 * Merge future lists
	 * @param result
	 */
	public void merge(final OperationFuture result) {
		futures.addAll(result.futures);
	}

}
