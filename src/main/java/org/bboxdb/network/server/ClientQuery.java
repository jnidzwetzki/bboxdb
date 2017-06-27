/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.network.server;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.bboxdb.distribution.RegionIdMapper;
import org.bboxdb.distribution.RegionIdMapperInstanceManager;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.MultipleTupleEndResponse;
import org.bboxdb.network.packages.response.MultipleTupleStartResponse;
import org.bboxdb.network.packages.response.PageEndResponse;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.CloseableIterator;
import org.bboxdb.storage.queryprocessor.QueryProcessor;
import org.bboxdb.storage.queryprocessor.queryplan.QueryPlan;
import org.bboxdb.storage.sstable.SSTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientQuery implements Closeable {

	/**
	 * The query plan to execute
	 */
	protected final QueryPlan queryPlan;
	
	/**
	 * Page the result
	 */
	protected final boolean pageResult;
	
	/**
	 * The amount of tuples per page
	 */
	protected final short tuplesPerPage;
	
	/**
	 * The local tables to query
	 */
	protected final List<SSTableName> localTables;
	
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
	protected final SSTableName requestTable;
	
	/**
	 * The current iterator
	 */
	protected CloseableIterator<Tuple> currentIterator;
	
	/**
	 * The total amount of send tuples
	 */
	protected long totalSendTuples;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ClientQuery.class);


	public ClientQuery(final QueryPlan queryPlan, final boolean pageResult,
			final short tuplesPerPage, final ClientConnectionHandler clientConnectionHandler, 
			final short querySequence, final SSTableName requestTable) {

		this.queryPlan = queryPlan;
		this.pageResult = pageResult;
		this.tuplesPerPage = tuplesPerPage;
		this.clientConnectionHandler = clientConnectionHandler;
		this.querySequence = querySequence;
		this.requestTable = requestTable;

		final RegionIdMapper nameprefixManager = RegionIdMapperInstanceManager.getInstance(requestTable.getDistributionGroupObject());
		this.localTables = nameprefixManager.getAllLocalTables(requestTable);
		
		this.totalSendTuples = 0;
	}

	
	public void closeIteratorNE() {
		if(currentIterator != null) {
			try {
				currentIterator.close();
				currentIterator = null;
			} catch (Exception e) {
				logger.warn("Got an exception while closing iterator", e);
			}
		}
	}
	
	/**
	 * Calculate the next tuples of the query
	 * @param packageSequence2 
	 * @return
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	public void fetchAndSendNextTuples(final short packageSequence) throws IOException, PackageEncodeException {

		long sendTuplesInThisPage = 0;
		clientConnectionHandler.writeResultPackage(new MultipleTupleStartResponse(packageSequence));
		
		while(! isDataExhausted()) {
			
			if(currentIterator == null) {
				setupNewIterator();
			}
			
			// Unable to set up a new iterator
			if(currentIterator == null) {
				break;
			}
			
			while(currentIterator.hasNext()) {
				// Handle page end
				if(pageResult == true && sendTuplesInThisPage >= tuplesPerPage) {
					clientConnectionHandler.writeResultPackage(new PageEndResponse(packageSequence));
					clientConnectionHandler.flushPendingCompressionPackages();
					return;
				}
				
				// Send next tuple
				final Tuple tuple = currentIterator.next();
				clientConnectionHandler.writeResultTuple(packageSequence, requestTable, tuple);
				totalSendTuples++;
				sendTuplesInThisPage++;
			}
			
			closeIteratorNE();
		}
		
		// All tuples are send
		clientConnectionHandler.writeResultPackage(new MultipleTupleEndResponse(packageSequence));	
		clientConnectionHandler.flushPendingCompressionPackages();
	}

	/**
	 * Is the data if the iterator exhausted?
	 * @return
	 */
	protected boolean isDataExhausted() {
		if(! localTables.isEmpty()) {
			return false;
		}
		
		if(currentIterator != null && currentIterator.hasNext()) {
			return false;
		}
		
		return true;
	}

	/**
	 * Setup a new iterator with the next local table
	 */
	protected boolean setupNewIterator() {
		if(currentIterator != null && ! currentIterator.hasNext()) {
			logger.warn("setupNewIterator() called, but old iterator is not exhaustet. Ignoring call");
			return false;
		}
		
		if(localTables.isEmpty()) {
			logger.warn("setupNewIterator() called, but localTables are empty");
			return false;
		}
		
		try {
			final SSTableName sstableName = localTables.remove(0);
			
			final SSTableManager storageManager = clientConnectionHandler
					.getStorageRegistry()
					.getSSTableManager(sstableName);
			
			final QueryProcessor queryProcessor = new QueryProcessor(queryPlan, storageManager);
			currentIterator = queryProcessor.iterator();
			return true;
		} catch (StorageManagerException e) {
			logger.warn("Got exception while fetching tuples", e);
		}
		
		return false;
	}
	
	/**
	 * Is the current query done
	 * @return
	 */
	public boolean isQueryDone() {
		return (currentIterator == null && localTables.isEmpty());
	}

	@Override
	public void close() {
		logger.debug("Closing query {} (send {} result tuples)", querySequence, totalSendTuples);
		closeIteratorNE();
	}

}