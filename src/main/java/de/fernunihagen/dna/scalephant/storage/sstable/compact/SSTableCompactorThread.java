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
package de.fernunihagen.dna.scalephant.storage.sstable.compact;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.distribution.regionsplit.RegionSplitStrategy;
import de.fernunihagen.dna.scalephant.distribution.regionsplit.RegionSplitStrategyFactory;
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableWriter;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableFacade;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableKeyIndexReader;
import de.fernunihagen.dna.scalephant.util.Acquirable;

public class SSTableCompactorThread implements Runnable {
	
	/**
	 * The corresponding SSTable manager
	 */
	protected final SSTableManager sstableManager;
	
	/**
	 * The merge strategy
	 */
	protected final MergeStrategy mergeStragegy;
	
	/**
	 * The name of the thread
	 */
	protected final String threadname;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableCompactorThread.class);

	public SSTableCompactorThread(final SSTableManager ssTableManager) {
		this.sstableManager = ssTableManager;
		this.mergeStragegy = new SimpleMergeStrategy();
		this.threadname = sstableManager.getSSTableName().getFullname();
	}

	/**
	 * Compact our SSTables
	 * 
	 */
	@Override
	public void run() {
		logger.info("Starting new compact thread for: {}", threadname);

		try {
			executeThread();
		} catch(Throwable e) {
			logger.error("Got an uncaught exception", e);
		}
		
		logger.info("Compact thread for: {} is done", threadname);
	}

	/**
	 * Execute the compactor thread
	 */
	protected void executeThread() {
		while(sstableManager.isReady()) {

			try {	
				Thread.sleep(mergeStragegy.getCompactorDelay());
				logger.debug("Executing compact thread for: {}", threadname);

				// Create a copy to ensure, that the list of facades don't change
				// during the compact run.
				final List<SSTableFacade> facades = new ArrayList<SSTableFacade>(sstableManager.getSstableFacades());
				final MergeTask mergeTask = mergeStragegy.getMergeTask(facades);
					
				try {
					mergeSSTables(mergeTask.getMinorCompactTables(), false);
					mergeSSTables(mergeTask.getMajorCompactTables(), true);				
				} catch (Exception e) {
					logger.error("Error while merging tables", e);
				} 
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} 
		}
		
		logger.info("Compact thread for: {} is done", threadname);
	}

	/**
	 * Calculate max the number of entries in the output
	 * @param tables
	 * @return
	 */
	public long calculateNumberOfEntries(final List<SSTableFacade> facades) {
		return facades
			.stream()
			.map(SSTableFacade::getSsTableKeyIndexReader)
			.mapToInt(SSTableKeyIndexReader::getNumberOfEntries)
			.sum();
	}

	/**
	 * Merge multipe facades into a new one
	 * @param reader1
	 * @param reader2
	 * @throws StorageManagerException
	 */
	protected void mergeSSTables(final List<SSTableFacade> facades, final boolean majorCompaction) throws StorageManagerException {
	
		if(facades == null || facades.isEmpty()) {
			return;
		}
		
		final String directory = facades.get(0).getDirectory();
		final SSTableName name = facades.get(0).getName();
		
		final long estimatedMaxNumberOfEntries = calculateNumberOfEntries(facades);
		final int tablenumber = sstableManager.increaseTableNumber();
		final SSTableWriter writer = new SSTableWriter(directory, name, tablenumber, estimatedMaxNumberOfEntries);
		
		final List<SSTableKeyIndexReader> reader = new ArrayList<SSTableKeyIndexReader>();
		for(final SSTableFacade facade : facades) {
			reader.add(facade.getSsTableKeyIndexReader());
		}
		
		// Log the compact call
		if(logger.isInfoEnabled()) {
			writeMergeLog(facades, tablenumber, majorCompaction);
		}
		
		// Run the compact process
		final SSTableCompactor ssTableCompactor = new SSTableCompactor(reader, writer);
		ssTableCompactor.setMajorCompaction(majorCompaction);
		final boolean compactSuccess = ssTableCompactor.executeCompactation();
		
		if(! compactSuccess) {
			logger.error("Error during compactation");
			return;
		} else {
			final float mergeFactor = (float) ssTableCompactor.getWrittenTuples() / (float) ssTableCompactor.getReadTuples();
			
			logger.info("Compactation done. Read {} tuples, wrote {} tuples (expected {}). Factor {}", 
					ssTableCompactor.getReadTuples(), ssTableCompactor.getWrittenTuples(), 
					estimatedMaxNumberOfEntries, mergeFactor);
		}
		
		registerNewFacadeAndDeleteOldInstances(facades, directory, name, tablenumber);
		
		if(majorCompaction) {
			testAndPerformTableSplit(ssTableCompactor.getWrittenTuples());
		}
	}

	/**
	 * Test and perform an table split if needed
	 * @param totalWrittenTuples 
	 */
	protected void testAndPerformTableSplit(final int totalWrittenTuples) {
		
		logger.info("Test for table split: " + sstableManager.getSSTableName().getFullname() 
				+ " total tuples: " + totalWrittenTuples);
		
		final RegionSplitStrategy splitter = RegionSplitStrategyFactory.getInstance();
		
		if(splitter.isSplitNeeded(totalWrittenTuples)) {
			splitter.performSplit(sstableManager.getSSTableName());
		}
		
	}

	/**
	 * Register a new sstable facade and delete the old ones
	 * @param tables
	 * @param directory
	 * @param name
	 * @param tablenumber
	 * @throws StorageManagerException
	 */
	protected void registerNewFacadeAndDeleteOldInstances(final List<SSTableFacade> tables, final String directory,
			final SSTableName name, final int tablenumber) throws StorageManagerException {
		// Create a new facade and remove the old ones
		final SSTableFacade newFacade = new SSTableFacade(directory, name, tablenumber);
		newFacade.init();
		
		// Register the new sstable reader
		sstableManager.getSstableFacades().add(newFacade);

		// Unregister and delete the files
		for(final Acquirable facade : tables) {
			facade.deleteOnClose();
			sstableManager.getSstableFacades().remove(facade);
		}
	}

	/***
	 * Write info about the merge run into log
	 * @param facades
	 * @param tablenumber
	 */
	protected void writeMergeLog(final List<SSTableFacade> facades, final int tablenumber, 
			final boolean majorCompaction) {
		
		final String formatedFacades = facades
				.stream()
				.mapToInt(SSTableFacade::getTablebumber)
				.mapToObj(Integer::toString)
				.collect(Collectors.joining(",", "[", "]"));
		
		logger.info("Merging (major: {}) {} into {}", majorCompaction, formatedFacades, tablenumber);
	}
}
