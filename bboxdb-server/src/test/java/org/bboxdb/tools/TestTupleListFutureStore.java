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
package org.bboxdb.tools;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.commons.RejectedException;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.client.tools.TupleListFutureStore;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.duplicateresolver.DoNothingDuplicateResolver;
import org.junit.Assert;
import org.junit.Test;

public class TestTupleListFutureStore {

	@Test(timeout=1000)
	public void testEmptyFutureStoreWait() throws InterruptedException {
		final TupleListFutureStore tupleListFutureStore = new TupleListFutureStore();
		Assert.assertTrue(tupleListFutureStore.getServiceState().isInRunningState());
		tupleListFutureStore.waitForCompletion();
		tupleListFutureStore.shutdown();
		Assert.assertTrue(tupleListFutureStore.getServiceState().isInShutdownState());
	}
	
	@Test
	public void testDefaultValues() {
		final TupleListFutureStore tupleListFutureStore = new TupleListFutureStore();
		Assert.assertEquals(TupleListFutureStore.DEFAULT_MAX_QUEUE_SIZE, tupleListFutureStore.getMaxQueueSize());
		Assert.assertEquals(TupleListFutureStore.DEFAULT_REQUEST_WORKER, tupleListFutureStore.getRequestWorker());
		tupleListFutureStore.shutdown();
	}
	
	@Test(expected=RejectedException.class)
	public void testRejectedExeption() throws InterruptedException, RejectedException {
		final TupleListFutureStore tupleListFutureStore = new TupleListFutureStore();
		tupleListFutureStore.shutdown();
		tupleListFutureStore.put(new TupleListFuture(new DoNothingDuplicateResolver()));
	}

	@Test
	public void testOneFuture() throws InterruptedException, RejectedException {
		final TupleListFutureStore tupleListFutureStore = new TupleListFutureStore();
		final TestTupleListFuture testFuture = new TestTupleListFuture();
		Assert.assertEquals(testFuture.getIteratorCalls(), 0);

		tupleListFutureStore.put(testFuture);
		
		System.out.println("Wait for completion");
		tupleListFutureStore.waitForCompletion();
		System.out.println("Shutdown...");
		tupleListFutureStore.shutdown();
		
		Assert.assertEquals(testFuture.getIteratorCalls(), TestTupleListFuture.ELEMENTS);
	}
	
	@Test
	public void testFiftyFutures() throws InterruptedException, RejectedException {
		final TupleListFutureStore tupleListFutureStore = new TupleListFutureStore();

		final List<TestTupleListFuture> futures = new ArrayList<>();
		for(int i = 0; i < 50; i++) {
			final TestTupleListFuture testFuture = new TestTupleListFuture();
			tupleListFutureStore.put(testFuture);
			futures.add(testFuture);
		}
		
		System.out.println("Wait for completion");
		tupleListFutureStore.waitForCompletion();
		System.out.println("Shutdown...");
		tupleListFutureStore.shutdown();
		
		final boolean hasNonFinishedIterators = futures
				.stream()
				.anyMatch(f -> f.getIteratorCalls() != TestTupleListFuture.ELEMENTS);
		
		Assert.assertFalse(hasNonFinishedIterators);
	}
	
}

/**
 * Simple test future implementation
 *
 */
class TestTupleListFuture extends TupleListFuture {
	
	public TestTupleListFuture() {
		super(new DoNothingDuplicateResolver());
	}

	public final static int ELEMENTS = 100;
	
	protected int iteratorCalls = 0;
	
	public int getIteratorCalls() {
		return iteratorCalls;
	}
	
	@Override
	public boolean isDone() {
		return true;
	}
	
	@Override
	public boolean waitForAll() throws InterruptedException {
		return true;
	}
	
	@Override
	public Iterator<Tuple> iterator() {
		return new Iterator<Tuple>() {

			@Override
			public boolean hasNext() {
				return iteratorCalls < ELEMENTS;
			}

			@Override
			public Tuple next() {
				iteratorCalls++;
				return null;
			}
			
		};
	}
}
