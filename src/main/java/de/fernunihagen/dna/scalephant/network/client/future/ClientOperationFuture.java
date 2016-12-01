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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientOperationFuture implements OperationFuture {
	
	/**
	 * The id of the operation
	 */
	protected short requestId;
	
	/**
	 * The result of the operation
	 */
	protected volatile Object operationResult = null;
	
	/**
	 * The mutex for sync operations
	 */
	protected final Object mutex = new Object();
	
	/**
	 * The error flag for the operation
	 */
	protected boolean failed = false;
	
	/**
	 * Empty constructor
	 */
	public ClientOperationFuture() {
	}
	
	/**
	 * Constructor with the request id
	 * @param requestId
	 */
	public ClientOperationFuture(final short requestId) {
		this.requestId = requestId;
	}

	@Override
	public boolean isDone() {
		return operationResult != null;
	}

	@Override
	public Object get(final int resultId) throws InterruptedException, ExecutionException {
		
		if(resultId != 0) {
			throw new ExecutionException("Unable to getID != 0 : " + resultId, new IllegalArgumentException());
		}
		
		synchronized (mutex) {
			while(operationResult == null && ! failed) {
				mutex.wait();
			}
		}
		
		return operationResult;
	}

	@Override
	public Object get(final int resultId, final long timeout, final TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		
		if(resultId != 0) {
			throw new ExecutionException("Unable to getID != 0 : " + resultId, new IllegalArgumentException());
		}
		
		synchronized (mutex) {
			while(operationResult == null && ! failed) {
				mutex.wait(unit.toMillis(timeout));
			}
		}
		
		return operationResult;
	}

	/**
	 * Returns the request id
	 */
	@Override
	public short getRequestId(final int resultId) {
		
		if(resultId != 0) {
			throw new IllegalArgumentException("Unable to getID != 0 : " + resultId);
		}
		
		return requestId;
	}

	/**
	 * Set the result of the operation
	 */
	@Override
	public void setOperationResult(final int resultId, final Object result) {
		
		if(resultId != 0) {
			throw new IllegalArgumentException("Unable to getID != 0 : " + resultId);
		}
		
		synchronized (mutex) {
			this.operationResult = result;
			mutex.notifyAll();
		}
	}

	/**
	 * Set the ID of the request
	 */
	@Override
	public void setRequestId(final int resultId, final short requestId) {
		
		if(resultId != 0) {
			throw new IllegalArgumentException("Unable to getID != 0 : " + resultId);
		}
		
		this.requestId = requestId;
	}

	/**
	 * Is the operation successful
	 * @return
	 */
	public boolean isFailed() {
		return failed;
	}

	/**
	 * Set the error flag for the operation
	 */
	public void setFailedState(final boolean notify) {
		failed = true;
		
		if(notify == true) {
			// Future is failed, wake up all blocked get() calls
			synchronized (mutex) {
				mutex.notify();
			}
		}
	}

	@Override
	public int getNumberOfResultObjets() {
		return 1;
	}

	@Override
	public boolean waitForAll() throws InterruptedException, ExecutionException {
		for(int i = 0; i < getNumberOfResultObjets(); i++) {
			get(i);
		}
		
		return true;
	}
}
