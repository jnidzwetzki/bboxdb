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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegionIdMapper;
import org.bboxdb.distribution.DistributionRegionIdMapperManager;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.MultipleTupleEndResponse;
import org.bboxdb.network.packages.response.MultipleTupleStartResponse;
import org.bboxdb.network.packages.response.PageEndResponse;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.duplicateresolver.TupleDuplicateResolverFactory;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
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
public class KeyClientQuery implements ClientQuery {

	/**
	 * The key to query
	 */
	protected final String key;
	
	/**
	 * Page the result
	 */
	protected final boolean pageResult;
	
	/**
	 * The amount of tuples per page
	 */
	protected final short tuplesPerPage;

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
	 * The tuples for the given key
	 */
	protected final List<Tuple> tuplesForKey = new ArrayList<>();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(KeyClientQuery.class);


	public KeyClientQuery(final String key, final boolean pageResult, final short tuplesPerPage, 
			final ClientConnectionHandler clientConnectionHandler, 
			final short querySequence, final TupleStoreName requestTable) {
		
			this.key = key;
			this.pageResult = pageResult;
			this.tuplesPerPage = tuplesPerPage;
			this.clientConnectionHandler = clientConnectionHandler;
			this.querySequence = querySequence;
			this.requestTable = requestTable;
			
			this.totalSendTuples = 0;
			
			computeTuples();
	}
	
	/**
	 * Fetch the tuples for the given key and remove the duplicates
	 */
	protected void computeTuples() {
		try {
			final DistributionGroupName distributionGroupObject = requestTable.getDistributionGroupObject();

			final DistributionRegionIdMapper nameprefixManager 
				= DistributionRegionIdMapperManager.getInstance(distributionGroupObject);

			final List<TupleStoreName> localTables = nameprefixManager.getAllLocalTables(requestTable);
			
			for(final TupleStoreName tupleStoreName : localTables) {
				final TupleStoreManager storageManager = clientConnectionHandler
						.getStorageRegistry()
						.getTupleStoreManager(tupleStoreName);
				
				final List<Tuple> tuplesInTable = storageManager.get(key);
				tuplesForKey.addAll(tuplesInTable);
			}

			removeDuplicates(localTables);
		} catch (StorageManagerException e) {
			logger.error("Got an exception while fetching tuples for key " + key, e);
			tuplesForKey.clear();
		}
	}

	/**
	 * Remove the duplicates for the given key
	 * @param localTables
	 * @throws StorageManagerException
	 */
	protected void removeDuplicates(final List<TupleStoreName> localTables) throws StorageManagerException {
		
		// No local table is known, so no configuration is known
		if(localTables.isEmpty()) {
			return;
		}
		
		final TupleStoreManager storageManager = clientConnectionHandler
				.getStorageRegistry()
				.getTupleStoreManager(localTables.get(0));
		
		final DuplicateResolver<Tuple> duplicateResolver 
			= TupleDuplicateResolverFactory.build(storageManager.getTupleStoreConfiguration());
		
		duplicateResolver.removeDuplicates(tuplesForKey);
	}
	
	@Override
	public void fetchAndSendNextTuples(final short packageSequence) throws IOException, PackageEncodeException {
		
		long sendTuplesInThisPage = 0;
		clientConnectionHandler.writeResultPackage(new MultipleTupleStartResponse(packageSequence));
		
		final Iterator<Tuple> tupleListIterator = tuplesForKey.iterator();
		
		while(tupleListIterator.hasNext()) {
	 		if(pageResult == true && sendTuplesInThisPage >= tuplesPerPage) {
				clientConnectionHandler.writeResultPackage(new PageEndResponse(packageSequence));
				clientConnectionHandler.flushPendingCompressionPackages();
				return;
			}
			
			// Send next tuple
			final Tuple tuple = tupleListIterator.next();
			tupleListIterator.remove();
			
			final JoinedTuple joinedTuple = new JoinedTuple(tuple, requestTable.getFullname());
			
			clientConnectionHandler.writeResultTuple(packageSequence, joinedTuple);
			totalSendTuples++;
			sendTuplesInThisPage++;
		}
	
		// All tuples are send
		clientConnectionHandler.writeResultPackage(new MultipleTupleEndResponse(packageSequence));	
		clientConnectionHandler.flushPendingCompressionPackages();
	}

	@Override
	public boolean isQueryDone() {
		return tuplesForKey.isEmpty();
	}

	@Override
	public void close() {
		logger.debug("Closing query {} (send {} result tuples)", querySequence, totalSendTuples);
	}

	@Override
	public long getTotalSendTuples() {
		return totalSendTuples;
	}
}
