package de.fernunihagen.dna.jkn.scalephant.network.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientOperationFuture implements OperationFuture<Object> {
	
	/**
	 * The id of the operation
	 */
	protected short requestId;
	
	/**
	 * The result of the operation
	 */
	protected volatile Object operationResult = null;
	
	/**
	 * The mutex for sync operations
	 */
	protected final Object mutex = new Object();
	
	/**
	 * The error flag for the operation
	 */
	protected boolean failed = false;
	
	/**
	 * Empty constructor
	 */
	public ClientOperationFuture() {
	}
	
	/**
	 * Constructor with the request id
	 * @param requestId
	 */
	public ClientOperationFuture(final short requestId) {
		this.requestId = requestId;
	}
	
	/**
	 * Canceling is not supported
	 */
	@Override
	public boolean cancel(final boolean mayInterruptIfRunning) {
		return false;
	}

	/**
	 * Canceling is not supported
	 */
	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return operationResult != null;
	}

	@Override
	public Object get() throws InterruptedException, ExecutionException {
		synchronized (mutex) {
			while(operationResult == null && ! failed) {
				mutex.wait();
			}
		}
		
		return operationResult;
	}

	@Override
	public Object get(long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		
		synchronized (mutex) {
			while(operationResult == null && ! failed) {
				mutex.wait(unit.toMillis(timeout));
			}
		}
		
		return operationResult;
	}

	/**
	 * Returns the request id
	 */
	@Override
	public short getRequestId() {
		return requestId;
	}

	/**
	 * Set the result of the operation
	 */
	@Override
	public void setOperationResult(final Object result) {
		synchronized (mutex) {
			this.operationResult = result;
			mutex.notifyAll();
		}
	}

	/**
	 * Set the ID of the request
	 */
	@Override
	public void setRequestId(final short requestId) {
		this.requestId = requestId;
	}

	/**
	 * Is the operation successful
	 * @return
	 */
	public boolean isFailed() {
		return failed;
	}

	/**
	 * Set the error flag for the operation
	 */
	public void setFailedState() {
		failed = false;
		
		// Future is failed, wake up all blocked get() calls
		synchronized (mutex) {
			mutex.notify();
		}
	}
}
