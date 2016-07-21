package de.fernunihagen.dna.jkn.scalephant.network.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MultiClientOperationFuture implements OperationFuture {

	public final List<ClientOperationFuture> futures;
	
	public MultiClientOperationFuture() {
		futures = new ArrayList<ClientOperationFuture>();
	}
	
	/**
	 * Add a future to the result list
	 * @param clientOperationFuture
	 */
	public void addFuture(final ClientOperationFuture clientOperationFuture) {
		futures.add(clientOperationFuture);
	}

	@Override
	public void setRequestId(final int resultId, final short requestId) {
		if(resultId > futures.size()) {
			throw new IllegalArgumentException("Unable to access future with id: " + resultId + "(total " + futures.size() + ")");
		}
		
	}

	@Override
	public short getRequestId(final int resultId) {
		if(resultId > futures.size()) {
			throw new IllegalArgumentException("Unable to access future with id: " + resultId + "(total " + futures.size() + ")");
		}
		
		return futures.get(resultId).getRequestId(0);
	}

	@Override
	public void setOperationResult(final int resultId, final Object result) {
		if(resultId > futures.size()) {
			throw new IllegalArgumentException("Unable to access future with id: " + resultId + "(total " + futures.size() + ")");
		}
		
		futures.get(resultId).setOperationResult(0, result);
	}

	@Override
	public boolean isFailed() {
		
		for(ClientOperationFuture future : futures) {
			if(future.isFailed()) {
				return true;
			}
		}
		
		return false;
	}

	@Override
	public void setFailedState() {
		for(ClientOperationFuture future : futures) {
			future.setFailedState();
		}
	}

	@Override
	public boolean isDone() {
		for(ClientOperationFuture future : futures) {
			if(! future.isDone()) {
				return false;
			}
		}
		
		return true;
	}

	@Override
	public int getNumberOfResultObjets() {
		return futures.size();
	}

	@Override
	public Object get(final int resultId) throws InterruptedException, ExecutionException {
		
		if(resultId > futures.size()) {
			throw new IllegalArgumentException("Unable to access future with id: " + resultId + "(total " + futures.size() + ")");
		}
		
		return futures.get(resultId).get(0);
	}

	@Override
	public Object get(final int resultId, final long timeout, final TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		
		if(resultId > futures.size()) {
			throw new IllegalArgumentException("Unable to access future with id: " + resultId + "(total " + futures.size() + ")");
		}
		
		return futures.get(resultId).get(0, timeout, unit);
	}
	
	@Override
	public boolean waitForAll() throws InterruptedException, ExecutionException {
		for(int i = 0; i < getNumberOfResultObjets(); i++) {
			get(i);
		}
		
		return true;
	}

}
