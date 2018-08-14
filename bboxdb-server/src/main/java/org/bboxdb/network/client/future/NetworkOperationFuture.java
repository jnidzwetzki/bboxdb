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
	boolean isDone();

	/**
	 * Reexecute
	 */
	void execute();

	/**
	 * Get (and wait) for the result
	 * @return
	 * @throws InterruptedException
	 */
	Object get() throws InterruptedException;

	/**
	 * Get (and wait) for the result
	 * @param timeout
	 * @param unit
	 * @return
	 * @throws InterruptedException
	 * @throws TimeoutException 
	 */
	Object get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;

	/**
	 * Returns the request id
	 */
	short getRequestId();

	/**
	 * Set the result of the operation
	 */
	void setOperationResult(Object result);

	/**
	 * Is the operation successful
	 * @return
	 */
	boolean isFailed();

	/**
	 * Set the error flag for the operation
	 */
	void setFailedState();

	/**
	 * Wait for the completion of the future
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	boolean waitForCompletion() throws InterruptedException;

	/**
	 * Fire the completion event
	 */
	void fireCompleteEvent();

	/**
	 * Get the message of the result
	 * @return
	 */
	String getMessage();

	/**
	 * Set the message of the result
	 * @param message
	 */
	void setMessage(String message);

	/**
	 * Is the given result complete?
	 * @return
	 */
	boolean isCompleteResult();

	/**
	 * Set the complete flag
	 * @param complete
	 */
	void setCompleteResult(boolean complete);

	/**
	 * Get the needed time for task completion
	 * @return
	 */
	long getCompletionTime(TimeUnit timeUnit);

	/**
	 * Get the id of the connection
	 * @return
	 */
	BBoxDBConnection getConnection();

	/**
	 * The last transmitted package
	 * @return
	 */
	NetworkRequestPackage getTransmittedPackage();

	/**
	 * Get the message and the connection id in a human readable format
	 * @return
	 */
	String getMessageWithConnectionName();

	/**
	 * The error callback
	 * 
	 * @param errorCallback
	 */
	void setErrorCallback(FutureErrorCallback errorCallback);

	/**
	 * The success callback
	 * @param successCallback
	 */
	void setSuccessCallback(Consumer<NetworkOperationFutureImpl> successCallback);

	/**
	 * Get the number of executions
	 * @return
	 */
	int getExecutions();

}