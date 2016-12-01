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

public interface OperationFuture {
	
	/**
	 * Set the request id of the operation
	 * @return
	 */
	public void setRequestId(final int resultId, final short requestId);
	
	/**
	 * Get the request id of the operation
	 * @return
	 */
	public short getRequestId(final int resultId);
	
	/**
	 * Set the result of the operation
	 * @param result
	 */
	public void setOperationResult(final int resultId, final Object result);
	
	/**
	 * Is the future processed successfully or are errors occurred?
	 * @return
	 */
	public boolean isFailed();

	/**
	 * Set the failed state
	 */
	public void setFailedState(final boolean notify);
	
	/**
	 * Is the future done
	 */
	public boolean isDone();
	
	/**
	 * Get the number of result objects
	 * @return
	 */
	public int getNumberOfResultObjets();
	
	/**
	 * Get the result of the future
	 * @return
	 */
    public Object get(final int resultId) throws InterruptedException, ExecutionException;
    
    /**
	 * Get the result of the future
	 * @return
     * @throws TimeoutException 
	 */
    public Object get(final int resultId, final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;

    /**
     * Wait for all futures to complete
     * @return
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public boolean waitForAll() throws InterruptedException, ExecutionException;
}
