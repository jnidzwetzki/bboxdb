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
package org.bboxdb.test.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.NetworkOperationFutureImpl;
import org.bboxdb.network.client.tools.FixedSizeFutureStore;
import org.bboxdb.test.TestFuture;
import org.junit.Assert;
import org.junit.Test;

public class TestFixedFutureStore {

	/**
	 * Remove on empty list
	 */
	@Test(timeout=10_000)
	public void testRemove1() {
		final FixedSizeFutureStore futureStore = new FixedSizeFutureStore(10);
		futureStore.removeCompleteFutures();
	}

	/**
	 * Add more futures in failed state
	 */
	@Test(timeout=10_000)
	public void testTupleStore1() {
		final FixedSizeFutureStore futureStore = new FixedSizeFutureStore(10);

		for(int i = 0; i < 20; i++) {
			final EmptyResultFuture future = new EmptyResultFuture(() -> (new ArrayList<>()));
			futureStore.put(future);
		}
	}

	/**
	 * Add more futures in done state
	 */
	@Test(timeout=10_000)
	public void testTupleStore2() {
		final FixedSizeFutureStore futureStore = new FixedSizeFutureStore(10);

		// Fail test on failed callback
		futureStore.addFailedFutureCallback(c -> {Assert.assertTrue(false);});

		for(int i = 0; i < 20; i++) {
			final EmptyResultFuture future = new EmptyResultFuture(() -> (new ArrayList<>()));
			futureStore.put(future);
		}

		Assert.assertTrue(true);
	}

	/**
	 * Add more futures in failed state and count failed callbacks
	 * @throws InterruptedException
	 */
	@Test(timeout=10_000)
	public void testTupleStore3() throws InterruptedException {
		final FixedSizeFutureStore futureStore = new FixedSizeFutureStore(10);
		final AtomicInteger atomicInteger = new AtomicInteger(0);

		futureStore.addFailedFutureCallback(c -> {atomicInteger.incrementAndGet();});

		for(int i = 0; i < 20; i++) {
			final NetworkOperationFutureImpl networkOperationFuture = TestFuture.getFailingNetworkFuture();
			final EmptyResultFuture future = new EmptyResultFuture(() -> Arrays.asList(networkOperationFuture));
			futureStore.put(future);
		}

		futureStore.waitForCompletion();

		Assert.assertTrue(true);
		Assert.assertEquals(20, atomicInteger.get());
	}

	/**
	 * Test statistics write
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@Test(timeout=10_000)
	public void writeStatistics() throws InterruptedException, IOException {

		final File tempFile = File.createTempFile("test-", ".tmp");
		tempFile.deleteOnExit();

		final BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile));

		final FixedSizeFutureStore futureStore = new FixedSizeFutureStore(10);
		futureStore.writeStatistics(bw);

		for(int i = 0; i < 20; i++) {
			final NetworkOperationFutureImpl networkOperationFuture = TestFuture.getFailingNetworkFuture();
			final EmptyResultFuture future = new EmptyResultFuture(() -> Arrays.asList(networkOperationFuture));
			futureStore.put(future);
		}

		futureStore.waitForCompletion();

		bw.close();

		try (Stream<String> lines = Files.lines(tempFile.toPath(), Charset.defaultCharset())) {
			  final long numOfLines = lines.count();
			  Assert.assertEquals(20, numOfLines);
		}


	}
}
