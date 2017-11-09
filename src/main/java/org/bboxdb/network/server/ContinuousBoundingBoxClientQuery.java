package org.bboxdb.network.server;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

import org.bboxdb.distribution.RegionIdMapper;
import org.bboxdb.distribution.RegionIdMapperInstanceManager;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.MultipleTupleEndResponse;
import org.bboxdb.network.packages.response.MultipleTupleStartResponse;
import org.bboxdb.network.packages.response.PageEndResponse;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The key query is implemented in an own class, because the 
 * result can not be lazy evacuated from the tuple stores.
 * 
 * All tuples for a given key needs to be computed at once
 * so that the duplicates can be removed
 *
 */
public class ContinuousBoundingBoxClientQuery implements ClientQuery {

	/**
	 * The bounding box of the query
	 */
	protected final BoundingBox boundingBox;

	/**
	 * The client connection handler
	 */
	protected final ClientConnectionHandler clientConnectionHandler;
	
	/**
	 * The package sequence of the query
	 */
	protected final short querySequence;
	
	/**
	 * The request table
	 */
	protected final TupleStoreName requestTable;

	/**
	 * The total amount of send tuples
	 */
	protected long totalSendTuples;
	
	/**
	 * The number of tuples per page
	 */
	protected final long tuplesPerPage = 1;
	
	/**
	 * Is the continuous query active
	 */
	protected boolean queryActive = true;
	
	/**
	 * The tuples for the given key
	 */
	protected final BlockingQueue<Tuple> tupleQueue;
	
	/**
	 * The maximal queue capacity
	 */
	protected final static int MAX_QUEUE_CAPACITY = 1024;
	
	/**
	 * The tuple insert callback
	 */
	protected final Consumer<Tuple> tupleInsertCallback;

	/**
	 * The tuple store manager
	 */
	protected TupleStoreManager storageManager;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ContinuousBoundingBoxClientQuery.class);


	public ContinuousBoundingBoxClientQuery(final BoundingBox boundingBox,
			final ClientConnectionHandler clientConnectionHandler, 
			final short querySequence, final TupleStoreName requestTable) {
		
			this.boundingBox = boundingBox;
			this.clientConnectionHandler = clientConnectionHandler;
			this.querySequence = querySequence;
			this.requestTable = requestTable;
			this.tupleQueue = new ArrayBlockingQueue<>(MAX_QUEUE_CAPACITY);
			
			this.totalSendTuples = 0;
			
			// Add each tuple to our tuple queue
			this.tupleInsertCallback = (t) -> {
				
				// Is the tuple important for the query?
				if(! t.getBoundingBox().overlaps(boundingBox)) {
					return;
				}
				
				final boolean insertResult = tupleQueue.offer(t);
				
				if(! insertResult) {
					logger.error("Unable to add tuple to continuous query, queue is full");
				}
			};
			
			init();
	}
	
	/**
	 * Init the query
	 * @param storageRegistry 
	 */
	protected void init() {
		
		try {
			final TupleStoreManagerRegistry storageRegistry 
				= clientConnectionHandler.getStorageRegistry();
			
			final RegionIdMapper regionIdMapper 
				= RegionIdMapperInstanceManager.getInstance(requestTable.getDistributionGroupObject());
			
			final Collection<TupleStoreName> localTables 
				= regionIdMapper.getLocalTablesForRegion(boundingBox, requestTable);
			
			if(localTables.size() != 1) {
				logger.error("Got more than one table for the continuous query {}", localTables);
				close();
				return;
			}

			final TupleStoreName tupleStoreName = localTables.iterator().next();
			storageManager = storageRegistry.getTupleStoreManager(tupleStoreName);
			
			// Remove tuple store insert listener on connection close
			clientConnectionHandler.addConnectionClosedHandler((c) -> close());
		} catch (StorageManagerException e) {
			logger.error("Got an exception during query init", e);
			close();
		}
	}
	
	@Override
	public void fetchAndSendNextTuples(final short packageSequence) throws IOException, PackageEncodeException {
		
		try {
			long sendTuplesInThisPage = 0;
			clientConnectionHandler.writeResultPackage(new MultipleTupleStartResponse(packageSequence));
						
			while(queryActive) {
				if(sendTuplesInThisPage >= tuplesPerPage) {
					clientConnectionHandler.writeResultPackage(new PageEndResponse(packageSequence));
					clientConnectionHandler.flushPendingCompressionPackages();
					return;
				}
				
				// Send next tuple or wait
				final Tuple tuple = tupleQueue.take();
				
				clientConnectionHandler.writeResultTuple(packageSequence, requestTable, tuple);
				totalSendTuples++;
				sendTuplesInThisPage++;
			}
			
			// All tuples are send
			clientConnectionHandler.writeResultPackage(new MultipleTupleEndResponse(packageSequence));	
			clientConnectionHandler.flushPendingCompressionPackages();
			
		} catch (InterruptedException e) {
			logger.debug("Got interrupted excetion");
			close();
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public boolean isQueryDone() {
		return (! queryActive);
	}

	@Override
	public void close() {
		logger.debug("Closing query {} (send {} result tuples)", querySequence, totalSendTuples);
	
		if(storageManager != null) {
			storageManager.removeInsertCallback(tupleInsertCallback);
		}
		
		queryActive = false;
	}

	@Override
	public long getTotalSendTuples() {
		return totalSendTuples;
	}
}
