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
package org.bboxdb.test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.bboxdb.commons.StacktraceHelper;
import org.bboxdb.network.client.future.client.FutureRetryPolicy;
import org.bboxdb.network.client.future.client.OperationFuture;
import org.bboxdb.network.client.future.client.OperationFutureImpl;
import org.bboxdb.network.client.future.network.NetworkOperationFuture;
import org.bboxdb.network.client.future.network.NetworkOperationFutureImpl;
import org.bboxdb.network.packets.NetworkRequestPacket;
import org.junit.Assert;
import org.junit.Test;

public class TestFuture {

	/**
	 * Get the retries for the test
	 */
	private final static int RETRIES_IN_TEST = 5;	
	
	@Test(timeout=60000)
	public void testNoRetry1() throws InterruptedException {
		final NetworkOperationFutureImpl networkFuture = getFailingNetworkFuture();

		final OperationFutureImpl<Boolean> future = new OperationFutureImpl<>(() -> Arrays.asList(networkFuture),
				FutureRetryPolicy.RETRY_POLICY_NONE);

		future.waitForCompletion();
		Assert.assertTrue(future.isDone());
		Assert.assertTrue(future.isFailed());
		Assert.assertEquals(1, networkFuture.getExecutions());
	}

	@Test(timeout=60000)
	public void testNoRetry2() throws InterruptedException {
		final NetworkOperationFuture networkFuture1 = getFailingNetworkFuture();
		final NetworkOperationFuture networkFuture2 = getReadyNetworkFuture();

		final Supplier<List<NetworkOperationFuture>> supplier
			= () -> (Arrays.asList(networkFuture1, networkFuture2));

		final OperationFutureImpl<Boolean> future = new OperationFutureImpl<>(supplier,
				FutureRetryPolicy.RETRY_POLICY_NONE);

		future.waitForCompletion();
		Assert.assertTrue(future.isDone());
		Assert.assertTrue(future.isFailed());
		Assert.assertEquals(1, networkFuture1.getExecutions());
		Assert.assertEquals(1, networkFuture2.getExecutions());
	}

	@Test(timeout=60000)
	public void testOneRetry1() throws InterruptedException {
		final NetworkOperationFutureImpl networkFuture = getFailingNetworkFuture();

		final OperationFutureImpl<Boolean> future = new OperationFutureImpl<>(() -> Arrays.asList(networkFuture),
				FutureRetryPolicy.RETRY_POLICY_ONE_FUTURE);

		future.waitForCompletion();
		Assert.assertTrue(future.isDone());
		Assert.assertTrue(future.isFailed());
		Assert.assertEquals(networkFuture.getTotalRetries() + 1, networkFuture.getExecutions());
	}

	@Test(timeout=60000)
	public void testOneRetry2() throws InterruptedException {
		final NetworkOperationFutureImpl networkFuture1 = getFailingNetworkFuture();
		final NetworkOperationFutureImpl networkFuture2 = getReadyNetworkFuture();

		final Supplier<List<NetworkOperationFuture>> supplier
			= () -> (Arrays.asList(networkFuture1, networkFuture2));

		final OperationFutureImpl<Boolean> future = new OperationFutureImpl<>(supplier,
				FutureRetryPolicy.RETRY_POLICY_ONE_FUTURE);

		future.waitForCompletion();
		Assert.assertTrue(future.isDone());
		Assert.assertTrue(future.isFailed());
		Assert.assertEquals(networkFuture1.getTotalRetries() + 1, networkFuture1.getExecutions());
		Assert.assertEquals(1, networkFuture2.getExecutions());
	}


	@Test(timeout=60000)
	public void testAllRetry1() throws InterruptedException {
		final NetworkOperationFutureImpl networkFuture = getFailingNetworkFuture();

		final Supplier<List<NetworkOperationFuture>> supplier
		= () -> (Arrays.asList(networkFuture));
		
		final AtomicInteger errorCalls = new AtomicInteger(0);
		final Consumer<OperationFuture> errorConsumer = new Consumer<OperationFuture>() {

			@Override
			public void accept(OperationFuture c) {
				errorCalls.incrementAndGet();
			}
		};
		
		final AtomicInteger successCalls = new AtomicInteger(0);
		final Consumer<OperationFuture> sucessConsumer = new Consumer<OperationFuture>() {

			@Override
			public void accept(OperationFuture c) {
				System.out.println(StacktraceHelper.getFormatedStacktrace());
				successCalls.incrementAndGet();
			}
		};

		final OperationFutureImpl<Boolean> future = new OperationFutureImpl<>(supplier,
				FutureRetryPolicy.RETRY_POLICY_ALL_FUTURES, 
				Arrays.asList(sucessConsumer), Arrays.asList(errorConsumer));

		future.waitForCompletion();
		Assert.assertTrue(future.isDone());
		Assert.assertTrue(future.isFailed());
		Assert.assertEquals(networkFuture.getTotalRetries() + 1, networkFuture.getExecutions());
		Assert.assertEquals(1, errorCalls.get());
		Assert.assertEquals(0, successCalls.get());
	}
	
	@Test(timeout=60000)
	public void testAllRetry3() throws InterruptedException {
		final NetworkOperationFutureImpl networkFuture = getFailingNetworkFuture();

		final Supplier<List<NetworkOperationFuture>> supplier
		= () -> (Arrays.asList(networkFuture));
		
		final AtomicInteger errorCalls = new AtomicInteger(0);
		final Consumer<OperationFuture> errorConsumer = new Consumer<OperationFuture>() {

			@Override
			public void accept(OperationFuture c) {
				errorCalls.incrementAndGet();
			}
		};
		
		final AtomicInteger successCalls = new AtomicInteger(0);
		final Consumer<OperationFuture> sucessConsumer = new Consumer<OperationFuture>() {

			@Override
			public void accept(OperationFuture c) {
				System.out.println(StacktraceHelper.getFormatedStacktrace());
				successCalls.incrementAndGet();
			}
		};

		final OperationFutureImpl<Boolean> future = new OperationFutureImpl<>(supplier,
				FutureRetryPolicy.RETRY_POLICY_ALL_FUTURES);

		future.waitForCompletion();
		
		// Register late
		future.addSuccessCallbackConsumer(sucessConsumer);
		future.addFailureCallbackConsumer(errorConsumer);
		
		Assert.assertTrue(future.isDone());
		Assert.assertTrue(future.isFailed());
		Assert.assertEquals(networkFuture.getTotalRetries() + 1, networkFuture.getExecutions());
		Assert.assertEquals(1, errorCalls.get());
		Assert.assertEquals(0, successCalls.get());
	}

	@Test(timeout=60000)
	public void testAllRetry2() throws InterruptedException {
		final NetworkOperationFutureImpl networkFuture1 = getFailingNetworkFuture();
		final NetworkOperationFutureImpl networkFuture2 = getReadyNetworkFuture();

		final Supplier<List<NetworkOperationFuture>> supplier
			= () -> (Arrays.asList(networkFuture1, networkFuture2));

		final OperationFutureImpl<Boolean> future = new OperationFutureImpl<>(supplier,
				FutureRetryPolicy.RETRY_POLICY_ALL_FUTURES);

		future.waitForCompletion();
		Assert.assertTrue(future.isDone());
		Assert.assertTrue(future.isFailed());

		final int totalRetries = networkFuture1.getTotalRetries() + 1;

		final int executions1 = networkFuture1.getExecutions();
		final int executions2 = networkFuture2.getExecutions();

		Assert.assertTrue(executions1 == totalRetries || executions2 == totalRetries);
	}

	/**
	 * Get a failing network future
	 *
	 * @return
	 */
	public static NetworkOperationFutureImpl getFailingNetworkFuture() {
		final Supplier<NetworkRequestPacket> supplier = () -> (null);

		final NetworkOperationFutureImpl resultFuture = 
				new NetworkOperationFutureImpl(BBoxDBTestHelper.MOCKED_CONNECTION, supplier) {
			
			public void execute() {
				super.execute();
				setFailedState();
				fireCompleteEvent();
			};
		};
		
		resultFuture.setTotalRetries(RETRIES_IN_TEST);
		
		return resultFuture;
	}

	/**
	 * Get a new network future
	 *
	 * @return
	 */
	public static NetworkOperationFutureImpl getReadyNetworkFuture() {
		final Supplier<NetworkRequestPacket> supplier = () -> (null);

		final NetworkOperationFutureImpl resultFuture = 
				new NetworkOperationFutureImpl(BBoxDBTestHelper.MOCKED_CONNECTION, supplier) {
			
			public void execute() {
				super.execute();
				fireCompleteEvent();
			};
		};
		
		resultFuture.setTotalRetries(RETRIES_IN_TEST);
		
		return resultFuture;
	}
}
