package de.fernunihagen.dna.jkn.scalephant.network.client;

import java.util.concurrent.Future;

public interface OperationFuture<V> extends Future<V> {
	
	/**
	 * Set the request id of the operation
	 * @return
	 */
	public void setRequestId(final short requestId);
	
	/**
	 * Get the request id of the operation
	 * @return
	 */
	public short getRequestId();
	
	/**
	 * Set the result of the operation
	 * @param result
	 */
	public void setOperationResult(final V result);
	
	/**
	 * Is the future processed successfully or are errors occured?
	 * @return
	 */
	public boolean isFailed();

	/**
	 * Set the failed state
	 */
	public void setFailedState();

}
