package de.fernunihagen.dna.jkn.scalephant.network;

import java.util.concurrent.Future;

public interface OperationFuture<V> extends Future<V> {
	
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

}
