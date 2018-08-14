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
import java.util.function.Consumer;

import org.bboxdb.network.client.BBoxDBConnection;
import org.bboxdb.network.packages.NetworkRequestPackage;

public interface NetworkOperationFuture {

	/**
	 * Is the operation done?
	 * @return
	 */
	public boolean isDone();

	/**
	 * Reexecute
	 */
	public void execute();

	/**
	 * Get (and wait) for the result
	 * @return
	 * @throws InterruptedException
	 */
	public Object get() throws InterruptedException;

	/**
	 * Get (and wait) for the result
	 * @param timeout
	 * @param unit
	 * @return
	 * @throws InterruptedException
	 * @throws TimeoutException 
	 */
	public Object get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;

	/**
	 * Returns the request id
	 */
	public short getRequestId();

	/**
	 * Set the result of the operation
	 */
	public void setOperationResult(Object result);

	/**
	 * Is the operation successful
	 * @return
	 */
	public boolean isFailed();

	/**
	 * Set the error flag for the operation
	 */
	public void setFailedState();

	/**
	 * Wait for the completion of the future
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public boolean waitForCompletion() throws InterruptedException;

	/**
	 * Fire the completion event
	 */
	public void fireCompleteEvent();

	/**
	 * Get the message of the result
	 * @return
	 */
	public String getMessage();

	/**
	 * Set the message of the result
	 * @param message
	 */
	public void setMessage(String message);

	/**
	 * Is the given result complete?
	 * @return
	 */
	public boolean isCompleteResult();

	/**
	 * Set the complete flag
	 * @param complete
	 */
	public void setCompleteResult(boolean complete);

	/**
	 * Get the needed time for task completion
	 * @return
	 */
	public long getCompletionTime(TimeUnit timeUnit);

	/**
	 * Get the id of the connection
	 * @return
	 */
	public BBoxDBConnection getConnection();

	/**
	 * The last transmitted package
	 * @return
	 */
	public NetworkRequestPackage getTransmittedPackage();

	/**
	 * Get the message and the connection id in a human readable format
	 * @return
	 */
	public String getMessageWithConnectionName();

	/**
	 * The error callback
	 * 
	 * @param errorCallback
	 */
	public void setErrorCallback(FutureErrorCallback errorCallback);

	/**
	 * The success callback
	 * @param successCallback
	 */
	public void setSuccessCallback(Consumer<NetworkOperationFutureImpl> successCallback);

	/**
	 * Get the number of executions
	 * @return
	 */
	public int getExecutions();

}