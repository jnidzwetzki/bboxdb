/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb;

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.concurrent.ThreadHelper;
import org.junit.Assert;
import org.junit.Test;

public class TestThreadHelper {


	@Test(timeout=60000)
	public void testEmptyList() {
		final List<Thread> threads = new ArrayList<>();
		final List<Thread> result = ThreadHelper.stopThreads(threads);
		Assert.assertTrue(result.isEmpty());
	}
	
	@Test(timeout=60000)
	public void testThreadInterruptable() {
		final Thread thread = getInterruptibleThread();	
		final List<Thread> threads = new ArrayList<>();
		threads.add(thread);
		final List<Thread> result = ThreadHelper.stopThreads(threads);
		Assert.assertTrue(result.isEmpty());
	}
	
	@Test(timeout=20000)
	public void testThreadInterruptable2() {
		final Thread thread = getNonInterruptibleThread();	
		final List<Thread> threads = new ArrayList<>();
		threads.add(thread);
		
		// This call need a test with timeout, because otherwise
		// we will interrupt the regular testcase thread
		Thread.currentThread().interrupt();
		
		final List<Thread> result = ThreadHelper.stopThreads(threads);
		Assert.assertFalse(result.isEmpty());
	}
	
	@Test(timeout=20000)
	public void testThreadNonInterruptable() {
		final Thread thread = getNonInterruptibleThread();	
		final List<Thread> threads = new ArrayList<>();
		threads.add(thread);
		final List<Thread> result = ThreadHelper.stopThreads(threads);
		Assert.assertEquals(1, result.size());
		Assert.assertTrue(result.contains(thread));
	}

	/**
	 * Get a interruptible thread
	 * @return
	 */
	protected Thread getInterruptibleThread() {
		final Thread thread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}
		});
		thread.start();
		return thread;
	}

	/**
	 * Get a non interruptible thread. A non interruptible 
	 * task 10 * THREAD_WAIT_TIMEOUT
	 * @return
	 */
	protected Thread getNonInterruptibleThread() {
		final Thread thread = new Thread(new Runnable() {
			
			@Override
			public void run() {	
				for(int i = 0; i < 10; i++) {
					try {
						Thread.sleep(ThreadHelper.THREAD_WAIT_TIMEOUT);
					} catch (InterruptedException e) {
						// Ignore
					}
				}
			}
		});
		thread.start();
		return thread;
	}
	
}
