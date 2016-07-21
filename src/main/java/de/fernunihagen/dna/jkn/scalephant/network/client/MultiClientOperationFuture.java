package de.fernunihagen.dna.jkn.scalephant.network.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MultiClientOperationFuture extends ClientOperationFuture {

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
	
	/*
	 * *(non-Javadoc)
	 * @see de.fernunihagen.dna.jkn.scalephant.network.client.ClientOperationFuture#get()
	 */
	@Override
	public Object get() throws InterruptedException, ExecutionException {
		for(ClientOperationFuture future : futures) {
			final Object result = future.get();
		}
		
		return null;
	}
	
}
