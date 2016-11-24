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
package de.fernunihagen.dna.scalephant.storage.queryprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableFacade;

public class SSTableQueryProcessor {

	/**
	 * The predicate to evaluate
	 */
	protected final Predicate predicate;
	
	/**
	 * The sstable manager
	 */
	protected final SSTableManager ssTableManager;
	
	/**
	 * The list of aquired facades
	 */
	protected final List<SSTableFacade> aquiredFacades;
	
	/**
	 * Is the iterator ready?
	 */
	protected boolean ready;
	
	/**
	 * The seen tuples<Key, Timestamp> map
	 */
	protected Map<String, Long> seenTuples = new HashMap<String, Long>();

	/**
	 * The Logger
	 */
	protected static final Logger logger = LoggerFactory.getLogger(SSTableQueryProcessor.class);
	
	
	public SSTableQueryProcessor(final Predicate predicate, final SSTableManager ssTableManager) {
		this.predicate = predicate;
		this.ssTableManager = ssTableManager;
		this.aquiredFacades = new ArrayList<SSTableFacade>();
		this.ready = false;
	}
	
	public CloseableIterator<Tuple> iterator() {
		
		aquireTables();
		
		return new CloseableIterator<Tuple>() {

			@Override
			public boolean hasNext() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public Tuple next() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void close() throws Exception {
				releaseTables();
			}
			
		};
	}

	/**
	 * Try to aquire all needed tables
	 */
	protected void aquireTables() {
		final int retrys = 10;
		
		ready = false;
		
		for(int execution = 0; execution < retrys; execution++) {
			aquiredFacades.clear();
			boolean allTablesAquired = true;
			
			for(final SSTableFacade facade : ssTableManager.getSstableFacades()) {
				boolean canBeUsed = facade.acquire();
				
				if(! canBeUsed ) {
					allTablesAquired = false;
					break;
				}
				
				aquiredFacades.add(facade);
			}
			
			if(allTablesAquired == true) {
				ready = true;
				return;
			}
		}
		 
		logger.warn("Unable to aquire all sstables with {} retries", retrys);
	}
	
	/**
	 * Release all aquired tables
	 */
	protected void releaseTables() {
		ready = false;
		for(final SSTableFacade facade : aquiredFacades) {
			facade.release();
		}
	}


	
//	/**
//	 * Get all tuples that are inside of the bounding box
//	 * @param boundingBox
//	 * @return
//	 * @throws StorageManagerException 
//	 */
//	public Collection<Tuple> getTuplesInside(final BoundingBox boundingBox) throws StorageManagerException {
//	
//		final List<Tuple> resultList = new ArrayList<Tuple>();
//		
//		// Query memtable
//		final Collection<Tuple> memtableTuples = memtable.getTuplesInside(boundingBox);
//		resultList.addAll(memtableTuples);
//		
//		// Query unflushed memtables
//		for(final Memtable unflushedMemtable : unflushedMemtables) {
//			try {
//				final Collection<Tuple> memtableResult = unflushedMemtable.getTuplesInside(boundingBox);
//				resultList.addAll(memtableResult);
//			} catch (StorageManagerException e) {
//				logger.warn("Got an exception while scanning unflushed memtable: ", e);
//			}
//		}
//		
//		// Query the sstables
//		final List<Tuple> storedTuples = getTuplesInsideFromSStable(boundingBox);
//		resultList.addAll(storedTuples);
//		
//		return getTheMostRecentTuples(resultList);
//	}
//
//	/**
//	 * Get all tuples that are inside the given bounding box from the sstables
//	 * @param timestamp
//	 * @return
//	 */
//	protected List<Tuple> getTuplesInsideFromSStable(final BoundingBox boundingBox) {
//		
//		boolean readComplete = false;
//		final List<Tuple> storedTuples = new ArrayList<Tuple>();
//
//		while(! readComplete) {
//			readComplete = true;
//			storedTuples.clear();
//			
//			// Read data from the persistent SSTables
//			for(final SSTableFacade facade : sstableFacades) {
//				boolean canBeUsed = facade.acquire();
//				
//				if(! canBeUsed ) {
//					readComplete = false;
//					break;
//				}
//				
//				final SSTableKeyIndexReader indexReader = facade.getSsTableKeyIndexReader();
//								
//				for (final Tuple tuple : indexReader) {
//					if(tuple.getBoundingBox().overlaps(boundingBox)) {
//						storedTuples.add(tuple);
//					}
//				}
//				
//				facade.release();
//			}
//		}
//		return storedTuples;
//	}
//
//	@Override
//	public Collection<Tuple> getTuplesAfterTime(final long timestamp)
//			throws StorageManagerException {
//	
//		final List<Tuple> resultList = new ArrayList<Tuple>();
//		
//		// Query active memtable
//		final Collection<Tuple> memtableTuples = memtable.getTuplesAfterTime(timestamp);
//		resultList.addAll(memtableTuples);
//
//		// Query unflushed memtables
//		for(final Memtable unflushedMemtable : unflushedMemtables) {
//			try {
//				final Collection<Tuple> memtableResult = unflushedMemtable.getTuplesAfterTime(timestamp);
//				resultList.addAll(memtableResult);
//			} catch (StorageManagerException e) {
//				logger.warn("Got an exception while scanning unflushed memtable: ", e);
//			}
//		}
//		
//		// Query sstables
//		final List<Tuple> storedTuples = getTuplesAfterTimeFromSSTable(timestamp);
//		resultList.addAll(storedTuples);
//		
//		return getTheMostRecentTuples(resultList);
//	}*/
//
//	/**
//	 * Get all tuples that are newer than the given timestamp from the sstables
//	 * @param timestamp
//	 * @return
//	 */
//	protected List<Tuple> getTuplesAfterTimeFromSSTable(final long timestamp) {
//		
//		// Scan the sstables
//		boolean readComplete = false;
//		final List<Tuple> storedTuples = new ArrayList<Tuple>();
//
//		while(! readComplete) {
//			readComplete = true;
//			storedTuples.clear();
//			
//			// Read data from the persistent SSTables
//			for(final SSTableFacade facade : sstableFacades) {
//				boolean canBeUsed = facade.acquire();
//				
//				if(! canBeUsed ) {
//					readComplete = false;
//					break;
//				}
//				
//				// Scan only tables that contain newer tuples
//				if(facade.getSsTableMetadata().getNewestTuple() > timestamp) {
//					final SSTableKeyIndexReader indexReader = facade.getSsTableKeyIndexReader();
//					for (final Tuple tuple : indexReader) {
//						if(tuple.getTimestamp() > timestamp) {
//							storedTuples.add(tuple);
//						}
//					}
//				}
//				
//				facade.release();
//			}
//		}
//		return storedTuples;
//	}
//	
//	/**
//	 * Get the a collection with the most recent version of the tuples
//	 * DeletedTuples are removed from the result set
//	 * 
//	 * @param tupleList
//	 * @return
//	 */
//	protected Collection<Tuple> getTheMostRecentTuples(final Collection<Tuple> tupleList) {
//		
//		final HashMap<String, Tuple> allTuples = new HashMap<String, Tuple>();
//
//		// Find the most recent version of the tuple
//		for(final Tuple tuple : tupleList) {
//			
//			final String tupleKey = tuple.getKey();
//			
//			if(! allTuples.containsKey(tupleKey)) {
//				allTuples.put(tupleKey, tuple);
//			} else {
//				final long knownTimestamp = allTuples.get(tupleKey).getTimestamp();
//				final long newTimestamp = tuple.getTimestamp();
//				
//				// Update with an newer version
//				if(newTimestamp > knownTimestamp) {
//					allTuples.put(tupleKey, tuple);
//				}
//			}
//		}
//		
//		// Remove deleted tuples from result
//		for(final Tuple tuple : allTuples.values()) {
//			if(tuple instanceof DeletedTuple) {
//				allTuples.remove(tuple.getKey());
//			}
//		}
//		
//		return allTuples.values();
//	}
	
	

}
