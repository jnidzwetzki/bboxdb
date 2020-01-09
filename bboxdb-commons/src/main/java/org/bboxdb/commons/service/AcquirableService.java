/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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
package org.bboxdb.commons.service;

import java.util.concurrent.atomic.AtomicInteger;

import org.bboxdb.commons.concurrent.AcquirableResource;

public class AcquirableService extends ServiceState implements AcquirableResource {
	
	/**
	 * The usage counter
	 */
	private final AtomicInteger usageCounter;
	
	public AcquirableService() {
		super();
		this.usageCounter = new AtomicInteger(0);
	}

	@Override
	public boolean acquire() {
		
		if(! isInRunningState()) {
			return false;
		}
		
		synchronized (usageCounter) {
			usageCounter.incrementAndGet();
			usageCounter.notifyAll();
		}
		
		return true;
	}

	@Override
	public void release() {
		assert (usageCounter.get() > 0) : "Usage counter is 0";
		
		synchronized (usageCounter) {
			usageCounter.decrementAndGet();
			usageCounter.notifyAll();
		}
	}
	
	/**
	 * Get the usage counter
	 * 
	 * @return
	 */
	public int getUsageCounter() {
		return usageCounter.get();
	}
	
	/**
	 * Wait until the resource is unused
	 * @throws InterruptedException
	 */
	public void waitUntilUnused() throws InterruptedException {
		synchronized (usageCounter) {
			while(usageCounter.get() > 0) {
				usageCounter.wait();
			}
		}
	}

}
