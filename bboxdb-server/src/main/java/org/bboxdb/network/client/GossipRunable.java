package org.bboxdb.network.client;

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.concurrent.ExceptionSafeThread;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GossipRunable extends ExceptionSafeThread {

	/**
	 * The tuple in the last keep alive gossip call
	 */
	private final List<Tuple> lastGossipTuples;
	
	/**
	 * The table name of the last gossip tuples
	 */
	private final String lastGossipTableName;
	
	/**
	 * The BBoxDB client
	 */
	private final BBoxDBClient bboxDBClient;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(GossipRunable.class);

	public GossipRunable(final BBoxDBClient bboxDBClient) {
		this(new ArrayList<>(), "", bboxDBClient);
	}
	
	public GossipRunable(final List<Tuple> lastGossipTuples, final String lastGossipTableName,
			final BBoxDBClient bboxDBClient) {
		
		this.lastGossipTuples = lastGossipTuples;
		this.lastGossipTableName = lastGossipTableName;
		this.bboxDBClient = bboxDBClient;
	}

	@Override
	protected void runThread() throws Exception {
		final EmptyResultFuture resultFuture 
			= bboxDBClient.sendKeepAlivePackage(
					lastGossipTableName, 
					lastGossipTuples);
		
		waitForResult(resultFuture);
	}
	
	/**
	 * @param resultFuture 
	 * @param resultFuture
	 * @throws InterruptedException
	 */
	private void waitForResult(final EmptyResultFuture resultFuture) throws InterruptedException {
		
		// Wait for our keep alive to be processed
		resultFuture.waitForAll();
		
		// Gossip has detected an outdated version
		if(resultFuture.isFailed()) {
			
			if(lastGossipTuples == null || lastGossipTableName == null) {
				logger.error("Falied keep alive, but no idea what was our last gossip");
				return;
			}
			
			logger.info("Got failed message back from keep alive, "
					+ "outdated tuples detected by gossip {} / {}", lastGossipTableName, lastGossipTuples);
			
			for(final Tuple tuple: lastGossipTuples) {
				try {
					bboxDBClient.insertTuple(lastGossipTableName, tuple);
				} catch (BBoxDBException e) {
					logger.error("Got Exception while performing gossip repair", e);
				}
			}
		}
	}


	
}
