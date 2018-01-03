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

import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

import org.bboxdb.commons.ServiceState;
import org.bboxdb.commons.ServiceState.State;
import org.junit.Assert;
import org.junit.Test;

public class TestServiceState {

	/**
	 * Normal dispatching
	 * @throws InterruptedException 
	 */
	@Test(timeout=1000)
	public void testTransition1() throws InterruptedException {
		final ServiceState state = new ServiceState();
		Assert.assertFalse(state.isInFinishedState());

		state.dipatchToStarting();
		state.awaitStarting();
		Assert.assertFalse(state.isInRunningState());
		state.dispatchToRunning();
		Assert.assertTrue(state.isInRunningState());
		state.awaitRunning();
		Assert.assertFalse(state.isInShutdownState());
		state.dispatchToStopping();
		Assert.assertTrue(state.isInShutdownState());
		state.awaitStopping();
		state.dispatchToTerminated();
		state.awaitTerminatedOrFailed();
		
		Assert.assertTrue(state.isInFinishedState());
	}
	
	/**
	 * Error dispatching
	 * @throws InterruptedException 
	 */
	@Test(timeout=1000, expected=IllegalStateException.class)
	public void testTransition2() throws InterruptedException {
		final ServiceState state = new ServiceState();
		Assert.assertFalse(state.isInShutdownState());
		state.dispatchToTerminated();
		Assert.assertTrue(state.isInShutdownState());
		state.awaitTerminatedOrFailed();
	}

	/**
	 * Error dispatching
	 * @throws InterruptedException 
	 */
	@Test(timeout=1000)
	public void testTransition3() throws InterruptedException {
		final ServiceState state = new ServiceState();
		final IllegalArgumentException exception = new IllegalArgumentException();
		state.dispatchToFailed(exception);
		state.awaitTerminatedOrFailed();
		Assert.assertTrue(state.isInFinishedState());
		Assert.assertFalse(state.isInRunningState());
	}
	
	/**
	 * Error dispatching
	 * @throws InterruptedException 
	 */
	@Test(timeout=1000)
	public void testTransition4() throws InterruptedException {
		final ServiceState state = new ServiceState();
		(new Thread(() -> {
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			state.dipatchToStarting();
			state.dispatchToRunning();
		})).start();
		
		state.awaitRunning();
		Assert.assertTrue(state.isInRunningState());
	}

	/**
	 * Error dispatching
	 * @throws InterruptedException 
	 */
	@Test(timeout=1000)
	public void testTransition5() throws InterruptedException {
		final ServiceState state = new ServiceState();
		final IllegalArgumentException exception = new IllegalArgumentException();
		state.dispatchToFailed(exception);
		Assert.assertTrue(state.isInFinishedState());
		Assert.assertEquals(exception, state.getThrowable());
		state.dispatchToRunning();
		state.dispatchToStopping();
		state.dispatchToRunning();
		state.dispatchToTerminated();
		Assert.assertTrue(state.isInFinishedState());
	}
	
	/**
	 * Force dispatch
	 * @throws InterruptedException 
	 */
	@Test(timeout=1000)
	public void testTransition6() throws InterruptedException {
		final ServiceState state = new ServiceState();
		Assert.assertFalse(state.isInFinishedState());
		Assert.assertFalse(state.isInTerminatedState());
		state.forceDispatchToTerminated();
		Assert.assertTrue(state.isInFinishedState());
		Assert.assertTrue(state.isInTerminatedState());
	}
	
	/**
	 * Test the to string method
	 */
	@Test
	public void testToString() {
		final ServiceState state = new ServiceState();
		final int length = state.toString().length();
		final IllegalArgumentException exception = new IllegalArgumentException();
		state.dispatchToFailed(exception);
		final int lengthWithException = state.toString().length();

		// Assume at least 100 chars for stacktrace
		Assert.assertTrue(length + 100 < lengthWithException);
	}
	
	/**
	 * Test the callback listener
	 */
	@Test(timeout=1000)
	public void testCallbackListener() {
		final Semaphore semaphore = new Semaphore(0);
		final ServiceState state = new ServiceState();
		
		state.registerCallback((s) -> { 
			if(s.getState() == State.TERMINATED) {
				semaphore.release();
			}
		});
		
		Assert.assertEquals(0, semaphore.availablePermits());
		state.dipatchToStarting();
		Assert.assertEquals(0, semaphore.availablePermits());
		state.dispatchToRunning();
		Assert.assertEquals(0, semaphore.availablePermits());
		state.dispatchToStopping();
		Assert.assertEquals(0, semaphore.availablePermits());
		state.dispatchToTerminated();
		Assert.assertEquals(1, semaphore.availablePermits());
	}
	
	/**
	 * Test the register and unregister method
	 */
	@Test
	public void testCallbackListenerRegisterAndUnregister() {
		final ServiceState state = new ServiceState();
		
		final Consumer<? super ServiceState> consumer = (s) -> { 
		
		};
		
		// Callback is unkown
		Assert.assertFalse(state.removeCallback(consumer));
		
		state.registerCallback(consumer);
		
		// Callback is known
		Assert.assertTrue(state.removeCallback(consumer));
		
		// Callback is unkown
		Assert.assertFalse(state.removeCallback(consumer));
	}

	
	/**
	 * Test the reset call
	 */
	@Test
	public void testReset() {
		final ServiceState state = new ServiceState();
		final IllegalArgumentException exception = new IllegalArgumentException();
		state.dispatchToFailed(exception);
		state.reset();
		Assert.assertEquals(null, state.getThrowable());
		Assert.assertTrue(state.isInNewState());
	}
	
}
