/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegionIdMapper;
import org.bboxdb.distribution.DistributionRegionIdMapperManager;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.MultipleTupleEndResponse;
import org.bboxdb.network.packages.response.MultipleTupleStartResponse;
import org.bboxdb.network.packages.response.PageEndResponse;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.queryprocessor.OperatorTreeBuilder;
import org.bboxdb.storage.queryprocessor.operator.Operator;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamClientQuery implements Closeable, ClientQuery {

	/**
	 * The query plan to execute
	 */
	protected final OperatorTreeBuilder operatorTreeBuilder;
	
	/**
	 * The active operator
	 */
	protected Operator activeOperator;

	/**
	 * The current iterator
	 */
	protected Iterator<JoinedTuple> activeOperatorIterator;
	
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
	protected final Map<TupleStoreName, List<TupleStoreName>> localTables;
	
	/**
	 * The client connection handler
	 */
	protected final ClientConnectionHandler clientConnectionHandler;
	
	/**
	 * The package sequence of the query
	 */
	protected final short querySequence;
	
	/**
	 * The total amount of send tuples
	 */
	protected long totalSendTuples;
	
	/**
	 * The request tables
	 */
	private final List<TupleStoreName> requestTables;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(StreamClientQuery.class);

	public StreamClientQuery(final OperatorTreeBuilder operatorTreeBuilder, final boolean pageResult,
			final short tuplesPerPage, final ClientConnectionHandler clientConnectionHandler, 
			final short querySequence, final List<TupleStoreName> requestTables) {

		this.operatorTreeBuilder = operatorTreeBuilder;
		this.pageResult = pageResult;
		this.tuplesPerPage = tuplesPerPage;
		this.clientConnectionHandler = clientConnectionHandler;
		this.querySequence = querySequence;
		this.requestTables = requestTables;
		this.localTables = new HashMap<TupleStoreName, List<TupleStoreName>>();

		determineLocalTables(requestTables);
		
		this.totalSendTuples = 0;
	}

	/**
	 * Determine the local tables
	 * @param requestTables
	 */
	private void determineLocalTables(final List<TupleStoreName> requestTables) {
		for(final TupleStoreName requestTable : requestTables) {
			final DistributionGroupName distributionGroupObject = requestTable.getDistributionGroupObject();
			final DistributionRegionIdMapper nameprefixManager = DistributionRegionIdMapperManager.getInstance(distributionGroupObject);
			final List<TupleStoreName> localTablesForTable = nameprefixManager.getAllLocalTables(requestTable);
			localTablesForTable.sort((c1, c2) -> c1.compareTo(c2));
			localTables.put(requestTable, localTablesForTable);
		}
		
		// Check all tables have the same amount of local tables
		int elementSize = -1;
		for(final List<TupleStoreName> elements : localTables.values()) {
			if(elementSize == -1) {
				elementSize = elements.size();
			}
			
			if(elementSize != elements.size()) {
				throw new IllegalArgumentException("Got invalid element size: " + elementSize + " / " + elements.size());
			}
		}
	}
	
	/**
	 * Get the number of tables to process
	 * @return
	 */
	private int getNumberOfTablesToProcess() {
		return localTables.get(requestTables.get(0)).size();
	}

	/**
	 * Close the iterator
	 */
	protected void closeIteratorNE() {
		CloseableHelper.closeWithoutException(activeOperator, 
				(e) -> logger.warn("Got an exception while closing operator", e)); 
		
		activeOperator = null;
		activeOperatorIterator = null;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.server.ClientQuery#fetchAndSendNextTuples(short)
	 */
	@Override
	public void fetchAndSendNextTuples(final short packageSequence) throws IOException, PackageEncodeException {

		long sendTuplesInThisPage = 0;
		clientConnectionHandler.writeResultPackage(new MultipleTupleStartResponse(packageSequence));
		
		while(! isDataExhausted()) {
			
			if(activeOperatorIterator == null) {
				setupNewIterator();
			}
			
			// Unable to set up a new iterator
			if(activeOperatorIterator == null) {
				break;
			}
			
			while(activeOperatorIterator.hasNext()) {
				// Handle page end
				if(pageResult == true && sendTuplesInThisPage >= tuplesPerPage) {
					clientConnectionHandler.writeResultPackage(new PageEndResponse(packageSequence));
					clientConnectionHandler.flushPendingCompressionPackages();
					return;
				}
				
				// Send next tuple
				final JoinedTuple tuple = activeOperatorIterator.next();
								
				clientConnectionHandler.writeResultTuple(packageSequence, tuple);
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
		if(getNumberOfTablesToProcess() > 0) {
			return false;
		}
		
		if(activeOperatorIterator != null && activeOperatorIterator.hasNext()) {
			return false;
		}
		
		return true;
	}

	/**
	 * Setup a new iterator with the next local table
	 */
	protected boolean setupNewIterator() {
		if(activeOperatorIterator != null && ! activeOperatorIterator.hasNext()) {
			logger.warn("setupNewIterator() called, but old iterator is not exhaustet. Ignoring call");
			return false;
		}
		
		if(getNumberOfTablesToProcess() == 0) {
			logger.warn("setupNewIterator() called, but localTables are empty");
			return false;
		}
		
		try {
			final List<TupleStoreManager> storageManagers = new ArrayList<>();
			
			for(final TupleStoreName tupleStoreName : requestTables) {
				final TupleStoreName sstableName = localTables.get(tupleStoreName).remove(0);
				
				final TupleStoreManager storageManager = clientConnectionHandler
						.getStorageRegistry()
						.getTupleStoreManager(sstableName);
				
				storageManagers.add(storageManager);
			}
			
			activeOperator = operatorTreeBuilder.buildOperatorTree(storageManagers);
			activeOperatorIterator = activeOperator.iterator();
			return true;
		} catch (StorageManagerException e) {
			logger.warn("Got exception while fetching tuples", e);
		}
		
		return false;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.server.ClientQuery#isQueryDone()
	 */
	@Override
	public boolean isQueryDone() {
		return (activeOperatorIterator == null && getNumberOfTablesToProcess() == 0);
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.server.ClientQuery#close()
	 */
	@Override
	public void close() {
		logger.debug("Closing query {} (send {} result tuples)", querySequence, totalSendTuples);
		closeIteratorNE();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.server.ClientQuery#getTotalSendTuples()
	 */
	@Override
	public long getTotalSendTuples() {
		return totalSendTuples;
	}
}