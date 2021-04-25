/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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

import java.util.concurrent.CountDownLatch;

import org.bboxdb.commons.concurrent.BlockingQueueWithSingleExecutor;
import org.junit.Assert;
import org.junit.Test;

public class TestBlockingQueueWithSingleExecutor {

	@Test(timeout = 10_000)
	public void testQueue1() throws InterruptedException {
		final Thread testRunnerThread = Thread.currentThread();
		final CountDownLatch countDownLatch = new CountDownLatch(25);
		final BlockingQueueWithSingleExecutor executor = new BlockingQueueWithSingleExecutor(10);
		
		for(int i = 0; i < 25; i++) {
			Assert.assertTrue(executor.queue(() -> {
				countDownLatch.countDown();	
				
				// Ensure execution is performed in another thread
				Assert.assertFalse(Thread.currentThread().equals(testRunnerThread));
			}));
		}
		
		countDownLatch.await();
		
		executor.shutdown();
		
		Assert.assertFalse(executor.queue(() -> countDownLatch.countDown()));
	}
	
	@Test(timeout = 10_000)
	public void testQueue2() throws InterruptedException {
		final BlockingQueueWithSingleExecutor executor = new BlockingQueueWithSingleExecutor(10);
		Assert.assertTrue(executor.isExecutorActive());
		executor.shutdown();
		
		// Wait for shutdown
		while(executor.isExecutorActive()) {
			Thread.sleep(100);
		}
		
		Assert.assertFalse(executor.isExecutorActive());
	}
	
}
