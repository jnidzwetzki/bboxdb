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
package org.bboxdb;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.bboxdb.network.client.future.OperationFuture;
import org.bboxdb.network.client.future.OperationFutureImpl;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.sstable.duplicateresolver.DoNothingDuplicateResolver;
import org.junit.Assert;
import org.junit.Test;

public class TestFuture {

	/**
	 * Test the empty future
	 */
	@Test
	public void testEmptyFuture() {
		final OperationFuture operationFuture = new OperationFutureImpl<Object>();
		Assert.assertFalse(operationFuture.isFailed());
		Assert.assertTrue(operationFuture.isDone());
	}
	
	/**
	 * Test the failed operation
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testFutureFailureState() throws InterruptedException, ExecutionException {
		final OperationFutureImpl<Object> future = new OperationFutureImpl<Object>(1);
		Assert.assertFalse(future.isFailed());
		Assert.assertFalse(future.isDone());
		future.setFailedState();
		future.fireCompleteEvent();
		Assert.assertTrue(future.isFailed());
		Assert.assertTrue(future.isDone());
		Assert.assertTrue(future.get(0) == null);
	}
	
	/**
	 * Test the done state
	 */
	@Test
	public void testFutureDoneState() {
		final OperationFutureImpl<Object> future = new OperationFutureImpl<Object>(1);
		future.fireCompleteEvent();
		Assert.assertFalse(future.isFailed());
		Assert.assertTrue(future.isDone());
	}
	
	/**
	 * Test multi future done
	 */
	@Test
	public void testMultiFutureDoneState1() {
		final OperationFutureImpl<Object> future1 = new OperationFutureImpl<Object>(1);
		final OperationFutureImpl<Object> future2 = new OperationFutureImpl<Object>(1);
		
		final OperationFutureImpl<Object> mergedFuture = new OperationFutureImpl<Object>(0);
		mergedFuture.merge(future1);
		mergedFuture.merge(future2);

		Assert.assertFalse(mergedFuture.isFailed());
		Assert.assertFalse(mergedFuture.isDone());
		
		future1.fireCompleteEvent();
		Assert.assertFalse(mergedFuture.isFailed());
		Assert.assertFalse(mergedFuture.isDone());
		
		future2.fireCompleteEvent();
		Assert.assertFalse(mergedFuture.isFailed());
		Assert.assertTrue(mergedFuture.isDone());
	}
	
	/**
	 * Test multi future done
	 */
	@Test
	public void testMultiFutureDoneState2() {
		final OperationFutureImpl<Object> future1 = new OperationFutureImpl<Object>(1);
		final OperationFutureImpl<Object> future2 = new OperationFutureImpl<Object>(1);
		
		final OperationFutureImpl<Object> mergedFuture = new OperationFutureImpl<Object>(0);
		mergedFuture.merge(future1);
		mergedFuture.merge(future2);

		Assert.assertFalse(mergedFuture.isFailed());
		Assert.assertFalse(mergedFuture.isDone());
		
		mergedFuture.fireCompleteEvent();
		Assert.assertFalse(mergedFuture.isFailed());
		Assert.assertTrue(mergedFuture.isDone());
	}

	/**
	 * Test multi future failed
	 */
	@Test
	public void testMultiFutureFailedState1() {
		final OperationFutureImpl<Object> future1 = new OperationFutureImpl<Object>(1);
		final OperationFutureImpl<Object> future2 = new OperationFutureImpl<Object>(1);
		
		final OperationFutureImpl<Object> mergedFuture = new OperationFutureImpl<Object>(0);
		mergedFuture.merge(future1);
		mergedFuture.merge(future2);

		Assert.assertFalse(mergedFuture.isFailed());
		Assert.assertFalse(mergedFuture.isDone());
		
		future1.setFailedState();
		Assert.assertTrue(mergedFuture.isFailed());
		Assert.assertFalse(mergedFuture.isDone());
		
		future1.fireCompleteEvent();
		future2.fireCompleteEvent();
		Assert.assertTrue(mergedFuture.isFailed());
		Assert.assertTrue(mergedFuture.isDone());
	}
	
	/**
	 * Test multi future failed
	 */
	@Test
	public void testMultiFutureFailedState2() {
		final OperationFutureImpl<Object> future1 = new OperationFutureImpl<Object>(1);
		final OperationFutureImpl<Object> future2 = new OperationFutureImpl<Object>(1);
		
		final OperationFutureImpl<Object> mergedFuture = new OperationFutureImpl<Object>(0);
		mergedFuture.merge(future1);
		mergedFuture.merge(future2);

		Assert.assertFalse(mergedFuture.isFailed());
		Assert.assertFalse(mergedFuture.isDone());
		
		mergedFuture.setFailedState();
		mergedFuture.fireCompleteEvent();

		Assert.assertTrue(mergedFuture.isFailed());
		Assert.assertTrue(mergedFuture.isDone());
		
		Assert.assertTrue(future1.isFailed());
		Assert.assertTrue(future1.isDone());

		Assert.assertTrue(future2.isFailed());
		Assert.assertTrue(future2.isDone());
	}
	
	/**
	 * Test the timeout method
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws TimeoutException
	 */
	@Test(timeout=3000, expected=TimeoutException.class)
	public void testTimeout() throws InterruptedException, ExecutionException, TimeoutException {
		final OperationFutureImpl<Object> future1 = new OperationFutureImpl<Object>(1);
		future1.get(0, 1, TimeUnit.SECONDS);
	}
	
	/**
	 * Test the tuple list future
	 */
	@Test
	public void testTupleListFuture() {
		final TupleListFuture tupleListFuture = new TupleListFuture(2, new DoNothingDuplicateResolver());
		
		Assert.assertFalse(tupleListFuture.isCompleteResult(0));
		Assert.assertFalse(tupleListFuture.isCompleteResult(1));
		
		tupleListFuture.setCompleteResult(0, true);
		tupleListFuture.setCompleteResult(1, true);

		Assert.assertTrue(tupleListFuture.isCompleteResult(0));
		Assert.assertTrue(tupleListFuture.isCompleteResult(1));
	}
}
