package de.fernunihagen.dna.jkn.scalephant.network.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface OperationFuture {
	
	/**
	 * Set the request id of the operation
	 * @return
	 */
	public void setRequestId(final int resultId, final short requestId);
	
	/**
	 * Get the request id of the operation
	 * @return
	 */
	public short getRequestId(final int resultId);
	
	/**
	 * Set the result of the operation
	 * @param result
	 */
	public void setOperationResult(final int resultId, final Object result);
	
	/**
	 * Is the future processed successfully or are errors occurred?
	 * @return
	 */
	public boolean isFailed();

	/**
	 * Set the failed state
	 */
	public void setFailedState();
	
	/**
	 * Is the future done
	 */
	public boolean isDone();
	
	/**
	 * Get the number of result objects
	 * @return
	 */
	public int getNumberOfResultObjets();
	
	/**
	 * Get the result of the future
	 * @return
	 */
    public Object get(final int resultId) throws InterruptedException, ExecutionException;
    
    /**
	 * Get the result of the future
	 * @return
     * @throws TimeoutException 
	 */
    public Object get(final int resultId, final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;

    /**
     * Wait for all futures to complete
     * @return
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public boolean waitForAll() throws InterruptedException, ExecutionException;
}
