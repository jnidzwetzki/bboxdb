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

public interface OperationFuture {

	/**
	 * Set the request id of the operation
	 * @return
	 */
	public abstract void setRequestId(int resultId, short requestId);

	/**
	 * Get the request id of the operation
	 * @return
	 */
	public abstract short getRequestId(int resultId);

	/**
	 * Set the additional message
	 * @param resultId
	 * @param message
	 */
	public abstract void setMessage(int resultId, String message);

	/**
	 * Get the additional message
	 * @param resultId
	 * @return 
	 */
	public abstract String getMessage(int resultId);
	
	/**
	 * Get the additional messages from all results [message1, message2, ...]
	 * @return
	 */
	public abstract String getAllMessages();

	/**
	 * Is the future processed successfully or are errors occurred?
	 * @return
	 */
	public abstract boolean isFailed();

	/**
	 * Set the failed state
	 */
	public abstract void setFailedState();

	/**
	 * Is the future done
	 */
	public abstract boolean isDone();

	/**
	 * Get the number of result objects
	 * @return
	 */
	public abstract int getNumberOfResultObjets();

	/**
	 * Wait for all futures to complete
	 * @return
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public abstract boolean waitForAll() throws InterruptedException,
			ExecutionException;
	
	/**
	 * Fire the completion event
	 */
	public void fireCompleteEvent();
}