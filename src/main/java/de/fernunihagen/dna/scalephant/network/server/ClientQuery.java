/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.network.server;

import java.io.Closeable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.distribution.nameprefix.NameprefixInstanceManager;
import de.fernunihagen.dna.scalephant.distribution.nameprefix.NameprefixMapper;
import de.fernunihagen.dna.scalephant.network.packages.response.MultipleTupleEndResponse;
import de.fernunihagen.dna.scalephant.network.packages.response.MultipleTupleStartResponse;
import de.fernunihagen.dna.scalephant.network.packages.response.PageEndResponse;
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.StorageRegistry;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.queryprocessor.CloseableIterator;
import de.fernunihagen.dna.scalephant.storage.queryprocessor.predicate.Predicate;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;

public class ClientQuery implements Closeable {

	/**
	 * The predicate to execute
	 */
	protected final Predicate predicate;
	
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
	protected final short packageSequence;
	
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


	public ClientQuery(final Predicate predicate, final boolean pageResult,
			final short tuplesPerPage, final ClientConnectionHandler clientConnectionHandler, 
			final short packageSequence, final SSTableName requestTable) {

		this.predicate = predicate;
		this.pageResult = pageResult;
		this.tuplesPerPage = tuplesPerPage;
		this.clientConnectionHandler = clientConnectionHandler;
		this.packageSequence = packageSequence;
		this.requestTable = requestTable;

		final NameprefixMapper nameprefixManager = NameprefixInstanceManager.getInstance(requestTable.getDistributionGroupObject());
		this.localTables = nameprefixManager.getAllNameprefixesWithTable(requestTable);
		
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
	 * @return
	 */
	public void fetchAndSendNextTuples() {

		long sendTuplesInThisPage = 0;
		
		if(totalSendTuples == 0) {
			clientConnectionHandler.writeResultPackage(new MultipleTupleStartResponse(packageSequence));
		}
			
		while(! localTables.isEmpty()) {
			
			if(currentIterator == null) {
				setupNewIterator();
			}
			
			while(currentIterator.hasNext()) {
				
				// Handle page end
				if(pageResult == true && sendTuplesInThisPage >= tuplesPerPage) {
					clientConnectionHandler.writeResultPackage(new PageEndResponse(packageSequence));
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
			final SSTableManager storageManager = StorageRegistry.getSSTableManager(sstableName);
			currentIterator = storageManager.getMatchingTuples(predicate);
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
		logger.debug("Closing query {} (send {} result tuples)", packageSequence, totalSendTuples);
		closeIteratorNE();
	}

}