package org.bboxdb;

import java.util.concurrent.atomic.AtomicInteger;

import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.concurrent.ExceptionSafeRunnable;
import org.junit.Assert;
import org.junit.Test;


public class TestExceptionSafeRunnable {
	
	/**
	 * Test the runnable without an exception
	 * @throws InterruptedException 
	 */
	@Test(timeout=10000)
	public void testWithoutException() throws InterruptedException {
		
		final AtomicInteger runTheadCalls = new AtomicInteger(0);
		final AtomicInteger beginHookCalls = new AtomicInteger(0);
		final AtomicInteger endHookCalls = new AtomicInteger(0);
		final AtomicInteger afterExceptionCalls = new AtomicInteger(0);
		
		/**
		 * Because the methods are protected, we need a real mock here
		 * Mockito can't verify protected method calls
		 */
		final ExceptionSafeRunnable runnable = new ExceptionSafeRunnable() {
			
			@Override
			protected void runThread() throws Exception {
				runTheadCalls.incrementAndGet();
			}
			
			@Override
			protected void beginHook() {
				beginHookCalls.incrementAndGet();
			}
			
			@Override
			protected void endHook() {
				endHookCalls.incrementAndGet();
			}
			
			@Override
			protected void afterExceptionHook() {
				afterExceptionCalls.incrementAndGet();
			}
		};
		
		final Thread thread = new Thread(runnable);
		thread.start();
		thread.join();
		
		Assert.assertEquals(1, runTheadCalls.get());
		Assert.assertEquals(1, beginHookCalls.get());
		Assert.assertEquals(1, endHookCalls.get());
		Assert.assertEquals(0, afterExceptionCalls.get());
	}
	
	/**
	 * Test the runnable with an exception
	 * @throws InterruptedException 
	 */
	@Test(timeout=10000)
	public void testWithException() throws InterruptedException {
		final AtomicInteger runTheadCalls = new AtomicInteger(0);
		final AtomicInteger beginHookCalls = new AtomicInteger(0);
		final AtomicInteger endHookCalls = new AtomicInteger(0);
		final AtomicInteger afterExceptionCalls = new AtomicInteger(0);
		
		/**
		 * Because the methods are protected, we need a real mock here
		 * Mockito can't verify protected method calls
		 */
		final ExceptionSafeRunnable runnable = new ExceptionSafeRunnable() {
			
			@Override
			protected void runThread() throws Exception {
				runTheadCalls.incrementAndGet();
				throw new Exception("Exception");
			}
			
			@Override
			protected void beginHook() {
				beginHookCalls.incrementAndGet();
			}
			
			@Override
			protected void endHook() {
				endHookCalls.incrementAndGet();
			}
			
			@Override
			protected void afterExceptionHook() {
				afterExceptionCalls.incrementAndGet();
			}
		};
		
		final Thread thread = new Thread(runnable);
		thread.start();
		thread.join();
		
		Assert.assertEquals(1, runTheadCalls.get());
		Assert.assertEquals(1, beginHookCalls.get());
		Assert.assertEquals(0, endHookCalls.get());
		Assert.assertEquals(1, afterExceptionCalls.get());
	}
	
	@Test(timeout=60000)
	public void testDefaultImplementation1() throws InterruptedException {
		final ExceptionSafeRunnable runnable = new ExceptionSafeRunnable() {
			@Override
			protected void runThread() throws Exception {
				
			}
		};
		
		final Thread thread = new Thread(runnable);
		thread.start();
		thread.join();
	}
	
	@Test(timeout=60000)
	public void testDefaultImplementation2() throws InterruptedException {
		final ExceptionSafeRunnable runnable = new ExceptionSafeRunnable() {
			@Override
			protected void runThread() throws Exception {
				throw new RejectedException("Exception");
			}
		};
		
		final Thread thread = new Thread(runnable);
		thread.start();
		thread.join();
	}
	
}
