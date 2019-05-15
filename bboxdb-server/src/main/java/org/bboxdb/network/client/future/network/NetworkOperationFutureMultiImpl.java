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
package org.bboxdb.network.client.future.network;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.bboxdb.network.client.BBoxDBConnection;
import org.bboxdb.network.client.future.client.FutureErrorCallback;
import org.bboxdb.network.packages.NetworkRequestPackage;

public class NetworkOperationFutureMultiImpl implements NetworkOperationFuture {
	
	/**
	 * The futures
	 */
	private final List<NetworkOperationFuture> futures;
	
	/**
	 * The first completed future
	 */
	private volatile NetworkOperationFuture completeFuture = null;

	/**
	 * The original error callback
	 */
	private FutureErrorCallback errorCallback;

	/**
	 * The original success callback
	 */
	private Consumer<NetworkOperationFuture> successCallback;
	
	public NetworkOperationFutureMultiImpl(final List<NetworkOperationFuture> futures) {
		this.futures = futures;
		
		this.futures.forEach(f -> f.setErrorCallback(this::handleErrorCallback));
		this.futures.forEach(f -> f.setSuccessCallback(this::handleSuccessCallback));
	}
	
	public boolean handleErrorCallback(final NetworkOperationFuture future) {
		if(this.completeFuture != null) {
			// Ignore error
			return false;
		}
		
		if(errorCallback != null) {
			return errorCallback.handleError(future);
		}
		
		return false;
	}

	public synchronized void handleSuccessCallback(final NetworkOperationFuture future) {
		if(this.completeFuture == null) {
			this.completeFuture = future;
			
			// Cancel all other operations
			final List<NetworkOperationFuture> futuresToCancel = futures.stream()
				.filter(f -> ! f.equals(completeFuture))
				.collect(Collectors.toList());
			
			for(final NetworkOperationFuture futureToCancel : futuresToCancel) {
				final BBoxDBConnection connection = futureToCancel.getConnection();
				
				if(connection == null) {
					continue;
				}
				
				final short requestId = futureToCancel.getRequestId();
				
				connection.getBboxDBClient().cancelRequest(requestId);
			}
		}
		
		if(successCallback != null) {
			successCallback.accept(future);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#isDone()
	 */
	@Override
	public boolean isDone() {
		return futures.stream()
			.anyMatch(f -> f.isDone());
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#execute()
	 */
	@Override
	public void execute() {
		futures.forEach(f -> f.execute());
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#get()
	 */
	@Override
	public Object get(final boolean waitForCompletion) throws InterruptedException {
		if(! waitForCompletion) {
			return futures.get(0).get(waitForCompletion);
		}
		
		return getReadyFuture().get(waitForCompletion);
	}

	/**
	 * @return
	 */
	private NetworkOperationFuture getReadyFuture() {
		
		if(completeFuture == null) {
			throw new IllegalStateException("No future is ready");
		}
		
		return completeFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#getRequestId()
	 */
	@Override
	public short getRequestId() {
		 return getReadyFuture().getRequestId();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#setOperationResult(java.lang.Object)
	 */
	@Override
	public void setOperationResult(final Object result) {
		throw new IllegalArgumentException("Unable to set result on multi future");
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#isFailed()
	 */
	@Override
	public boolean isFailed() {
		return futures.stream()
				.anyMatch(f -> f.isFailed());
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#setFailedState()
	 */
	@Override
	public void setFailedState() {
		futures.forEach(f -> f.setFailedState());
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#fireCompleteEvent()
	 */
	@Override
	public void fireCompleteEvent() {
		throw new IllegalArgumentException("Unable to fireCompleteEvent on multi future");
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#getMessage()
	 */
	@Override
	public String getMessage() {
		return getReadyFuture().getMessage();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#setMessage(java.lang.String)
	 */
	@Override
	public void setMessage(String message) {
		throw new IllegalArgumentException("Unable to setMessage on multi future");
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#isCompleteResult()
	 */
	@Override
	public boolean isCompleteResult() {
		return getReadyFuture().isCompleteResult();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#setCompleteResult(boolean)
	 */
	@Override
	public void setCompleteResult(boolean complete) {
		throw new IllegalArgumentException("Unable to setCompleteResult on multi future");
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#getCompletionTime(java.util.concurrent.TimeUnit)
	 */
	@Override
	public long getCompletionTime(final TimeUnit timeUnit) {
		return getReadyFuture().getCompletionTime(timeUnit);
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#getConnection()
	 */
	@Override
	public BBoxDBConnection getConnection() {
		return getReadyFuture().getConnection();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#getTransmittedPackage()
	 */
	@Override
	public NetworkRequestPackage getTransmittedPackage() {
		return getReadyFuture().getTransmittedPackage();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#getMessageWithConnectionName()
	 */
	@Override
	public String getMessageWithConnectionName() {
		return getReadyFuture().getMessageWithConnectionName();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#setErrorCallback(org.bboxdb.network.client.future.FutureErrorCallback)
	 */
	@Override
	public void setErrorCallback(final FutureErrorCallback errorCallback) {
		this.errorCallback = errorCallback;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#setSuccessCallback(java.util.function.Consumer)
	 */
	@Override
	public void setSuccessCallback(final Consumer<NetworkOperationFuture> successCallback) {
		this.successCallback = successCallback;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#getExecutions()
	 */
	@Override
	public int getExecutions() {
		return getReadyFuture().getExecutions();
	}

}
