/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
package org.bboxdb.tools.network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.zookeeper.ContinuousQueryRegisterer;
import org.bboxdb.distribution.zookeeper.QueryEnlargement;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.client.EmptyResultFuture;
import org.bboxdb.network.client.tools.FixedSizeFutureStore;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.tools.converter.tuple.TupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketImporter implements Runnable {

	/**
	 * The port to open the server socket
	 */
	private int port;
	
	/**
	 * The BBoxDB connection point
	 */
	private String connectionPoint;
	
	/**
	 * The clustername
	 */
	private String clustername;

	/**
	 * The table to insert to
	 */
	private String table;
	
	/**
	 * The tuple decoder
	 */
	private TupleBuilder tupleFactory;
	
	/**
	 * The pending futures
	 */
	private final FixedSizeFutureStore pendingFutures;

	/**
	 * The Query enlargement
	 */
	private final QueryEnlargement enlargement;
	
	/**
	 * The amount of pending insert futures
	 */
	private final static int MAX_PENDING_FUTURES = 1000;
	
	/**
	 * The null table
	 */
	private final static String NULL_TABLE = "NULL";
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SocketImporter.class);

	public SocketImporter(final int port, final String connectionPoint, final String clustername, 
			final String table, final TupleBuilder tupleFactory, final QueryEnlargement enlargement) {
				this.port = port;
				this.connectionPoint = connectionPoint;
				this.clustername = clustername;
				this.table = table;
				this.tupleFactory = tupleFactory;
				this.enlargement = enlargement;
				this.pendingFutures = new FixedSizeFutureStore(MAX_PENDING_FUTURES);
	}

	@Override
	public void run() {

	    try(
	    		final ServerSocket serverSocket = new ServerSocket(port);
	    		final BBoxDB bboxdbClient = new BBoxDBCluster(connectionPoint, clustername);
	        ) {
	    	
			bboxdbClient.connect();
			
			while(! Thread.currentThread().isInterrupted()) {
				handleConnection(serverSocket, bboxdbClient);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			waitForPendingFutures();
		}   
	}

	/**
	 * Handle new socket connection
	 * @param serverSocket
	 * @param bboxdbClient
	 * @throws IOException
	 * @throws BBoxDBException
	 */
	private void handleConnection(final ServerSocket serverSocket, final BBoxDB bboxdbClient)
			throws IOException, BBoxDBException {
		
		logger.info("Ready and waiting for connections on port {}", port);
		
		try(
				final Socket clientSocket = serverSocket.accept();
				final InputStreamReader reader = new InputStreamReader(clientSocket.getInputStream());
				final BufferedReader inputStream = new BufferedReader(reader);
			){
			
			logger.info("Handle connection from: {}", clientSocket.getRemoteSocketAddress());

			String line;
			while ((line = inputStream.readLine()) != null) {
				final Tuple tuple = tupleFactory.buildTuple(line);
				
				if(tuple != null) {
					final Hyperrectangle tupleBBox = getEnlargedBBoxForTuple(tuple);
					tuple.setBoundingBox(tupleBBox);
					
					if(! NULL_TABLE.equals(table)) {
						final EmptyResultFuture result = bboxdbClient.insertTuple(table, tuple);
						pendingFutures.put(result);
					}
				}
			}
		} catch(Exception e) {
			logger.error("Got exception", e);
		}
	}

	/**
	 * Get the enlarged BBox for the tuple
	 * @param tuple
	 * @return
	 */
	private Hyperrectangle getEnlargedBBoxForTuple(final Tuple tuple) {
		
		if(enlargement == null) {
			return tuple.getBoundingBox();
		}
		
		final double maxAbsoluteEnlargement = enlargement.getMaxAbsoluteEnlargement();
		final double maxEnlargementFactor = enlargement.getMaxEnlargementFactor();
		final double maxEnlargementLat = enlargement.getMaxEnlargementLat();
		final double maxEnlargementLon = enlargement.getMaxEnlargementLon();
		
		final List<Hyperrectangle> bboxes = new ArrayList<>();
		bboxes.add(tuple.getBoundingBox());

		if(maxAbsoluteEnlargement != 0) {
			bboxes.add(tuple.getBoundingBox().enlargeByAmount(maxAbsoluteEnlargement));
		}
		
		if(maxEnlargementFactor != 1) {
			bboxes.add(tuple.getBoundingBox().enlargeByFactor(maxEnlargementFactor));
		}
		
		if(maxEnlargementLat != 0 || maxEnlargementLon != 0) {
			bboxes.add(tuple.getBoundingBox().enlargeByMeters(maxEnlargementLat, maxEnlargementLon));
		}
		
		final Hyperrectangle resultBox = Hyperrectangle.getCoveringBox(bboxes);
		
		// CLI print
		if(logger.isDebugEnabled()) {
			logger.debug("==========================");
			logger.debug("Enlargement: {}", enlargement);
			logger.debug("Original bounding box");
			logger.debug("{}", tuple.getBoundingBox());
			logger.debug("Enlarged bounding box");
			logger.debug("{}", resultBox);
			logger.debug("");
		}
		
		return resultBox;
	}

	/**
	 * Wait for the pending futures
	 */
	private void waitForPendingFutures() {
		try {
			pendingFutures.waitForCompletion();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	
	/**
	 * Main * Main * Main * Main
	 * @param args
	 * @throws InputParseException
	 */
	public static void main(final String[] args) throws InputParseException {
		
		if(args.length != 6) {
			System.err.println("Usage: <Class> <Port> <Connection Endpoint> <Clustername> <Table> <Format> <Enlargement>");
			System.exit(-1);
		}
		
		final String portString = args[0];
		final int port = MathUtil.tryParseInt(portString, () -> "Unable to parse: " + portString);
		final String connectionPoint = args[1];
		final String clustername = args[2];
		final String table = args[3];
		final String format = args[4];
		final String enlargement = args[5];
		
		final TupleBuilder tupleFactory = TupleBuilderFactory.getBuilderForFormat(format);
		QueryEnlargement queryEnlargement = null;
		
		// Read dynamic enlargement
		if("dynamic".equals(enlargement)) {
			final TupleStoreName tupleStoreName = new TupleStoreName(table);
			final ContinuousQueryRegisterer continuousQueryRegisterer = new ContinuousQueryRegisterer(tupleStoreName.getDistributionGroup(), tupleStoreName.getTablename());
			queryEnlargement = continuousQueryRegisterer.getMaxEnlagementFactorForTable();
		} else if(! "0".equals(enlargement)) {
			final double enlargementFactor = MathUtil.tryParseDoubleOrExit(enlargement, () -> "Unable to parse enlargement: " + enlargement);
			queryEnlargement = new QueryEnlargement();
			queryEnlargement.setMaxEnlargementFactor(enlargementFactor);
		}
		
		final SocketImporter main = new SocketImporter(port, connectionPoint, clustername, table, tupleFactory, queryEnlargement);
		main.run();
	}
}
