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
package org.bboxdb.commons;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class Retryer<T> {
	
	/**
	 * The retry mode
	 */
	public enum RetryMode {
		NORMAL,
		LINEAR,
		EXPONENTIAL
	}

	/**
	 * The amount of max executions
	 */
	private final int maxExecutions;
	
	/**
	 * The amount of needed executions
	 */
	private int neededExecutions;
	
	/**
	 * Number of milliseconds to wait between retries
	 */
	private final long waitMillis;
	
	/**
	 * The Callable
	 */
	private final Callable<T> callable;

	/**
	 * Is the task done
	 */
	private boolean done; 
	
	/**
	 * Is the task finished successfully
	 */
	private boolean successfully;
	
	/**
	 * The result of the operation
	 */
	private T result;
	
	/**
	 * The last thrown exception
	 */
	private final List<Exception> allExceptions;

	/**
	 * The retry mode
	 */
	private RetryMode retryMode;

	public Retryer(final int maxExecutions, final int waitTime, final TimeUnit timeUnit, 
			final Callable<T> callable) {
		
		this(maxExecutions, waitTime, timeUnit, callable, RetryMode.NORMAL);
	}
	
	public Retryer(final int maxExecutions, final int waitTime, final TimeUnit timeUnit, 
			final Callable<T> callable, final RetryMode retryMode) {
		
		this.maxExecutions = maxExecutions;
		this.waitMillis = timeUnit.toMillis(waitTime);
		this.callable = Objects.requireNonNull(callable);
		this.done = false;
		this.successfully = false;
		this.neededExecutions = 0;
		this.retryMode = retryMode;
		this.allExceptions = new ArrayList<>();
	}
	
	/**
	 * Execute the task
	 * @return
	 * @throws InterruptedException 
	 */
	public boolean execute() throws InterruptedException {

		for(neededExecutions = 1; neededExecutions < maxExecutions; neededExecutions++) {
			try {
				result = callable.call();
				successfully = true;
				break;
			} catch(Exception e) {
				allExceptions.add(e);
			}
			
			if(retryMode == RetryMode.NORMAL) {
				Thread.sleep(waitMillis);
			} else if(retryMode == RetryMode.LINEAR) {
				Thread.sleep(waitMillis * neededExecutions);
			} else {
				Thread.sleep((int) Math.pow(waitMillis, neededExecutions));
			}
		}
		
		done = true;
		
		return successfully;
	}
	
	/**
	 * Is the task done
	 * @return
	 */
	public boolean isDone() {
		return done;
	}
	
	/**
	 * Was the task successfully
	 * @return
	 */
	public boolean isSuccessfully() {
		return successfully;
	}
	
	/**
	 * Get the amount of max executions
	 * @return
	 */
	public int getMaxExecutions() {
		return maxExecutions;
	}
	
	/**
	 * Get the amount of needed executions
	 * @return
	 */
	public int getNeededExecutions() {
		if(! isDone()) {
			return 0;
		}
		
		return neededExecutions;
	}
	
	/**
	 * Get the result
	 * @return
	 */
	public T getResult() {
		return result;
	}
	
	/**
	 * Get the last exception
	 * @return
	 */
	public Exception getLastException() {
		if(allExceptions.isEmpty()) {
			return null;
		}
		
		return allExceptions.get(allExceptions.size() - 1);
	}
	
	/**
	 * Return all exceptions
	 * @return
	 */
	public List<Exception> getAllExceptions() {
		return Collections.unmodifiableList(allExceptions);
	}

	@Override
	public String toString() {
		return "Retryer [maxExecutions=" + maxExecutions + ", neededExecutions=" + neededExecutions + ", waitMillis="
				+ waitMillis + ", callable=" + callable + ", done=" + done + ", successfully=" + successfully
				+ ", result=" + result + ", allExceptions=" + allExceptions + ", retryMode=" + retryMode + "]";
	}
}
