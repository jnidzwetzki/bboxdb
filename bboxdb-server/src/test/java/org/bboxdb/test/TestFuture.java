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
import java.util.List;
import java.util.function.Supplier;

import org.bboxdb.network.client.BBoxDBConnection;
import org.bboxdb.network.client.future.client.FutureRetryPolicy;
import org.bboxdb.network.client.future.client.OperationFuture;
import org.bboxdb.network.client.future.client.OperationFutureImpl;
import org.bboxdb.network.client.future.network.NetworkOperationFuture;
import org.bboxdb.network.client.future.network.NetworkOperationFutureImpl;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFuture {

	/**
	 * The mocked connection
	 */
	private final static BBoxDBConnection MOCKED_CONNECTION = Mockito.mock(BBoxDBConnection.class);

	@BeforeClass
	public static void beforeClass() {
		Mockito.when(MOCKED_CONNECTION.isConnected()).thenReturn(true);
	}

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
		Assert.assertEquals(OperationFuture.TOTAL_RETRIES + 1, networkFuture.getExecutions());
	}

	@Test(timeout=60000)
	public void testOneRetry2() throws InterruptedException {
		final NetworkOperationFuture networkFuture1 = getFailingNetworkFuture();
		final NetworkOperationFuture networkFuture2 = getReadyNetworkFuture();

		final Supplier<List<NetworkOperationFuture>> supplier
			= () -> (Arrays.asList(networkFuture1, networkFuture2));

		final OperationFutureImpl<Boolean> future = new OperationFutureImpl<>(supplier,
				FutureRetryPolicy.RETRY_POLICY_ONE_FUTURE);

		future.waitForCompletion();
		Assert.assertTrue(future.isDone());
		Assert.assertTrue(future.isFailed());
		Assert.assertEquals(OperationFuture.TOTAL_RETRIES + 1, networkFuture1.getExecutions());
		Assert.assertEquals(1, networkFuture2.getExecutions());
	}


	@Test(timeout=60000)
	public void testAllRetry1() throws InterruptedException {
		final NetworkOperationFutureImpl networkFuture = getFailingNetworkFuture();

		final Supplier<List<NetworkOperationFuture>> supplier
		= () -> (Arrays.asList(networkFuture));

		final OperationFutureImpl<Boolean> future = new OperationFutureImpl<>(supplier,
				FutureRetryPolicy.RETRY_POLICY_ALL_FUTURES);

		future.waitForCompletion();
		Assert.assertTrue(future.isDone());
		Assert.assertTrue(future.isFailed());
		Assert.assertEquals(OperationFuture.TOTAL_RETRIES + 1, networkFuture.getExecutions());
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

		final int totalRetries = OperationFuture.TOTAL_RETRIES + 1;

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
		final Supplier<NetworkRequestPackage> supplier = () -> (null);

		return new NetworkOperationFutureImpl(MOCKED_CONNECTION, supplier) {
			public void execute() {
				super.execute();
				setFailedState();
				fireCompleteEvent();
			};
		};
	}

	/**
	 * Get a new network future
	 *
	 * @return
	 */
	public static NetworkOperationFutureImpl getReadyNetworkFuture() {
		final Supplier<NetworkRequestPackage> supplier = () -> (null);

		return new NetworkOperationFutureImpl(MOCKED_CONNECTION, supplier) {
			public void execute() {
				super.execute();
				fireCompleteEvent();
			};
		};
	}
}
