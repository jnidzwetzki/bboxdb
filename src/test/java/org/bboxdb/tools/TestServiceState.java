package org.bboxdb.tools;

import org.bboxdb.util.ServiceState;
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
		state.dispatchToFailed();
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
		state.dispatchToFailed();
		Assert.assertTrue(state.isInFinishedState());

		state.dispatchToRunning();
		state.dispatchToStopping();
		state.dispatchToRunning();
		state.dispatchToTerminated();
		Assert.assertTrue(state.isInFinishedState());
	}
	
}
