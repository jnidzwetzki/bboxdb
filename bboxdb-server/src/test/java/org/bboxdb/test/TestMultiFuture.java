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
package org.bboxdb.test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import org.bboxdb.network.client.future.NetworkOperationFuture;
import org.bboxdb.network.client.future.NetworkOperationFutureImpl;
import org.bboxdb.network.client.future.NetworkOperationFutureMultiImpl;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestMultiFuture {

	@Test(timeout=60000)
	public void testTwoNotFailed() throws InterruptedException {
		final NetworkOperationFuture future1 = Mockito.mock(NetworkOperationFuture.class);
		Mockito.when(future1.isFailed()).thenReturn(false);
		Mockito.when(future1.isDone()).thenReturn(true);
		
		final NetworkOperationFuture future2 = Mockito.mock(NetworkOperationFuture.class);
		Mockito.when(future2.isFailed()).thenReturn(false);
		Mockito.when(future2.isDone()).thenReturn(true);
		
		// One Future
		final NetworkOperationFutureMultiImpl multiFuture1 = new NetworkOperationFutureMultiImpl(
				Arrays.asList(future1));
		
		Assert.assertTrue(multiFuture1.isDone());
		Assert.assertFalse(multiFuture1.isFailed());
		
		// Two futures
		final NetworkOperationFutureMultiImpl multiFuture2 = new NetworkOperationFutureMultiImpl(
				Arrays.asList(future1, future2));
		
		Assert.assertTrue(multiFuture2.isDone());
		Assert.assertFalse(multiFuture2.isFailed());
	}
	
	@Test(timeout=60000)
	public void testTwoFailed() throws InterruptedException {
		final NetworkOperationFuture future1 = Mockito.mock(NetworkOperationFuture.class);
		Mockito.when(future1.isFailed()).thenReturn(false);
		Mockito.when(future1.isDone()).thenReturn(true);
		
		final NetworkOperationFuture future2 = Mockito.mock(NetworkOperationFuture.class);
		Mockito.when(future2.isFailed()).thenReturn(true);
		Mockito.when(future2.isDone()).thenReturn(true);
		
		// One Future
		final NetworkOperationFutureMultiImpl multiFuture1 = new NetworkOperationFutureMultiImpl(
				Arrays.asList(future1));
		
		Assert.assertTrue(multiFuture1.isDone());
		Assert.assertFalse(multiFuture1.isFailed());
		
		final NetworkOperationFutureMultiImpl multiFuture11 = new NetworkOperationFutureMultiImpl(
				Arrays.asList(future2));
		
		Assert.assertTrue(multiFuture11.isDone());
		Assert.assertTrue(multiFuture11.isFailed());
		
		// Two futures
		final NetworkOperationFutureMultiImpl multiFuture2 = new NetworkOperationFutureMultiImpl(
				Arrays.asList(future1, future2));
		
		Assert.assertTrue(multiFuture2.isDone());
		Assert.assertTrue(multiFuture2.isFailed());
	}
	
	@Test(timeout=60000)
	public void testSuccessCallback() throws InterruptedException {
		final NetworkOperationFutureImpl future1 = new NetworkOperationFutureImpl(null, null);
		final NetworkOperationFutureImpl future2 = new NetworkOperationFutureImpl(null, null);

		final NetworkOperationFutureMultiImpl multiFuture2 = new NetworkOperationFutureMultiImpl(
				Arrays.asList(future1, future2));
		
		final CountDownLatch latch = new CountDownLatch(1);
		
		multiFuture2.setSuccessCallback((c) -> {
			if(c == future1) {
				latch.countDown();
			}
		});
		
		Assert.assertFalse(multiFuture2.isDone());
		future1.fireCompleteEvent();
		Assert.assertTrue(multiFuture2.isDone());

		latch.await();
	}
	
}
