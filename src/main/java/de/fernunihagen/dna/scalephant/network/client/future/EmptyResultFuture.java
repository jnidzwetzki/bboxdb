package de.fernunihagen.dna.scalephant.network.client.future;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EmptyResultFuture extends OperationFuture<Boolean> {

	public EmptyResultFuture() {
		super();
	}

	public EmptyResultFuture(final int numberOfFutures) {
		super(numberOfFutures);
	}
	
	
	@Override
	public Boolean get(int resultId) throws InterruptedException, ExecutionException {
		
		// Wait for operation to complete
		waitForAll();
		
		// Return true, when the operation was succesfully
		return ! isFailed();
	}
	
	@Override
	public Boolean get(int resultId, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {

		// Wait for the future
		futures.get(resultId).get(timeout, unit);

		// Return true, when the operation was succesfully
		return ! isFailed();
	}

}
