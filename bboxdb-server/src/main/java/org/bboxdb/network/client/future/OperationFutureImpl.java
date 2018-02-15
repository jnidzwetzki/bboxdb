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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.bboxdb.network.client.BBoxDBClient;

public class OperationFutureImpl<T> implements OperationFuture {

	/**
	 * The futures
	 */
	protected final List<FutureImplementation<T>> futures;
	
	public OperationFutureImpl() {
		this(0);
	}
	
	public OperationFutureImpl(final int numberOfFutures) {
		futures = new ArrayList<FutureImplementation<T>>(numberOfFutures);
		
		for(int i = 0; i < numberOfFutures; i++) {
			futures.add(new FutureImplementation<T>());
		}
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#setRequestId(int, short)
	 */
	@Override
	public void setRequestId(final int resultId, final short requestId) {
		checkFutureSize(resultId);
		
		futures.get(resultId).setRequestId(requestId);
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#getRequestId(int)
	 */
	@Override
	public short getRequestId(final int resultId) {
		checkFutureSize(resultId);
		
		return futures.get(resultId).getRequestId();
	}

	/**
	 * Set the result of the operation
	 * @param result
	 */
	public void setOperationResult(final int resultId, final T result) {
		checkFutureSize(resultId);
		
		futures.get(resultId).setOperationResult(result);
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#setMessage(int, java.lang.String)
	 */
	@Override
	public void setMessage(final int resultId, final String message) {
		checkFutureSize(resultId);
		
		futures.get(resultId).setMessage(message);
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#getMessage(int)
	 */
	@Override
	public String getMessage(final int resultId) {
		checkFutureSize(resultId);
		
		return futures.get(resultId).getMessage();
	}
	
	@Override
	public void setConnection(final int resultId, final BBoxDBClient connection) {
		checkFutureSize(resultId);

		futures.get(resultId).setConnection(connection);
	}

	@Override
	public BBoxDBClient getConnection(final int resultId) {
		checkFutureSize(resultId);

		return futures.get(resultId).getConnection();
	}
	

	@Override
	public void setCompleteResult(int resultId, boolean completeResult) {
		checkFutureSize(resultId);
		
		futures.get(resultId).setCompleteResult(completeResult);
	}

	@Override
	public boolean isCompleteResult(int resultId) {
		checkFutureSize(resultId);
		
		return futures.get(resultId).isCompleteResult();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#getAllMessages()
	 */
	@Override
	public String getAllMessages() {
		return futures
		.stream()
		.filter(f -> f.getMessage() != null)
		.map(f -> f.getMessageWithConnectionName())
		.collect(Collectors.joining(",", "[", "]"));
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
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#isFailed()
	 */
	@Override
	public boolean isFailed() {
		return futures.stream().anyMatch(f -> f.isFailed());
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#setFailedState()
	 */
	@Override
	public void setFailedState() {
		for(final FutureImplementation<T> future : futures) {
			future.setFailedState();
		}
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#isDone()
	 */
	@Override
	public boolean isDone() {
		return futures.stream().allMatch(f -> f.isDone());
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#getNumberOfResultObjets()
	 */
	@Override
	public int getNumberOfResultObjets() {
		return futures.size();
	}

	/**
	 * Get the result of the future
	 * @return
	 */
	public T get(final int resultId) throws InterruptedException {
		checkFutureSize(resultId);
		
		return futures.get(resultId).get();
	}

    /**
	 * Get the result of the future
	 * @return
     * @throws TimeoutException 
	 */
	public T get(final int resultId, final long timeout, final TimeUnit unit)
			throws InterruptedException, TimeoutException {
		
		checkFutureSize(resultId);
		
		return futures.get(resultId).get(timeout, unit);
	}
	
    /* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#waitForAll()
	 */
	@Override
	public boolean waitForAll() throws InterruptedException {
		for(int i = 0; i < getNumberOfResultObjets(); i++) {
			get(i);
		}
		
		return true;
	}
	
    /* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#waitForAll()
	 */
	@Override
	public boolean waitForAll(final long timeout, final TimeUnit unit) 
			throws InterruptedException, TimeoutException {
		
		for(int i = 0; i < getNumberOfResultObjets(); i++) {
			get(i, timeout, unit);
		}
		
		return true;
	}

	/**
	 * Fire the completion event
	 */
	@Override
	public void fireCompleteEvent() {
		for(final FutureImplementation<T> future : futures) {
			future.fireCompleteEvent();
		}
	}

	/**
	 * Merge future lists
	 * @param result
	 */
	public void merge(final OperationFutureImpl<T> result) {
		futures.addAll(result.futures);
	}

	@Override
	public long getCompletionTime() {
		return futures.stream().mapToLong(f -> f.getCompletionTime()).sum();
	}

}
