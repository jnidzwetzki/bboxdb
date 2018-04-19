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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.bboxdb.network.client.BBoxDBConnection;

public interface OperationFuture {
		
	/**
	 * The number of retries
	 */
	public static final int TOTAL_RETRIES = 5;

	/**
	 * Get the request id of the operation
	 * @return
	 */
	public short getRequestId(final int resultId);

	/**
	 * Set the additional message
	 * @param resultId
	 * @param message
	 */
	public void setMessage(final int resultId, final String message);

	/**
	 * Get the additional message
	 * @param resultId
	 * @return 
	 */
	public String getMessage(final int resultId);

	/**
	 * Is the result complete?
	 * @param resultId
	 * @param complete
	 */
	public void setCompleteResult(final int resultId, final boolean complete);
	
	/**
	 * Is the given result complete?
	 * @param resultId
	 * @return
	 */
	public boolean isCompleteResult(final int resultId);
	
	/**
	 * Get the connection id from the future
	 */
	public BBoxDBConnection getConnection(final int resultId);
	
	/**
	 * Get the additional messages from all results [message1, message2, ...]
	 * @return
	 */
	public String getAllMessages();

	/**
	 * Is the future processed successfully or are errors occurred?
	 * @return
	 */
	public boolean isFailed();
	
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
	 * Wait for all futures to complete
	 * @return
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public void waitForCompletion() throws InterruptedException;
	
	/**
	 * Wait for the future to complete (with timeout)
	 * @param timeout
	 * @param unit
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws TimeoutException 
	 */
	public void waitForCompletion(final long timeout, final TimeUnit unit) 
			throws InterruptedException, TimeoutException;
	
	/**
	 * Get the time for completing the task
	 */
	public long getCompletionTime(final TimeUnit timeUnit);

}