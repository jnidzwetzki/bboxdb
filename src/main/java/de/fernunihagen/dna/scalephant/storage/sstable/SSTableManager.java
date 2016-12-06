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
package de.fernunihagen.dna.scalephant.storage.sstable;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.scalephant.ScalephantService;
import de.fernunihagen.dna.scalephant.storage.Memtable;
import de.fernunihagen.dna.scalephant.storage.ReadOnlyTupleStorage;
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.queryprocessor.CloseableIterator;
import de.fernunihagen.dna.scalephant.storage.queryprocessor.SSTableQueryProcessor;
import de.fernunihagen.dna.scalephant.storage.queryprocessor.predicate.Predicate;
import de.fernunihagen.dna.scalephant.storage.sstable.compact.SSTableCompactorThread;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableFacade;
import de.fernunihagen.dna.scalephant.util.State;
import de.fernunihagen.dna.scalephant.util.Stoppable;

public class SSTableManager implements ScalephantService {
	
	/**
	 * The name of the table
	 */
	protected final SSTableName sstablename;
	
	/**
	 * The tuple storre instances
	 */
	protected final TupleStoreInstances tupleStoreInstances;
	
	/**
	 * The Storage configuration
	 */
	protected final ScalephantConfiguration scalephantConfiguration;
	
	/**
	 * The number of the table
	 */
	protected AtomicInteger tableNumber;
	
	/**
	 * Ready flag for flush thread
	 */
	protected volatile boolean ready;
	
	/**
	 * The corresponding storage manager state
	 */
	protected State storageState;
	
	/**
	 * The running threads
	 */
	protected final Map<String, Thread> runningThreads;
	
	/**
	 * The stoppable tasks
	 */
	protected final Map<String, Stoppable> stoppableTasks;
	
	/**
	 * Id of the memtable flush thread
	 */
	protected final static String MEMTABLE_FLUSH_THREAD = "memtable";
	
	/**
	 * Id of the checkpoint thread
	 */
	protected final static String CHECKPOINT_THREAD = "checkpoint";
	
	/**
	 * Id of the compact thread
	 */
	protected final static String COMPACT_THREAD = "compact";
	
	/**
	 * The timeout for a thread join (10 seconds)
	 */
	protected long THREAD_WAIT_TIMEOUT = TimeUnit.SECONDS.toMillis(10);
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableManager.class);

	public SSTableManager(final SSTableName sstablename, final ScalephantConfiguration scalephantConfiguration) {
		super();

		this.scalephantConfiguration = scalephantConfiguration;
		this.storageState = new State(false); 
		this.sstablename = sstablename;
		this.tableNumber = new AtomicInteger();
		this.ready = false;
		
		this.tupleStoreInstances = new TupleStoreInstances();
		this.runningThreads = new HashMap<String, Thread>();
		this.stoppableTasks = new HashMap<String, Stoppable>();
	}

	/**
	 * Init the instance
	 * 
	 */
	@Override
	public void init() {
		
		if(ready == true) {
			logger.warn("SSTable manager is active and init() is called");
			return;
		}
		
		storageState.setReady(true);
		
		logger.info("Init a new instance for the table: " + getSSTableName());
		
		tupleStoreInstances.clear();
		runningThreads.clear();
		createSSTableDirIfNeeded();
		
		// Init the memtable before the sstablemanager. This ensures, that the
		// sstable recovery can put entries into the memtable
		initNewMemtable();
		
		try {
			scanForExistingTables();
		} catch (StorageManagerException e) {
			logger.error("Unable to init the instance: " + getSSTableName(), e);
			return;
		}
		
		tableNumber.set(getLastSequencenumberFromReader());

		// Set to ready before the threads are started
		ready = true;

		startMemtableFlushThread();
		startCompactThread();
		startCheckpointThread();
	}
	
	/**
	 * Create a new storage manager
	 */
	protected void initNewMemtable() {
		final Memtable memtable = new Memtable(sstablename, 
				scalephantConfiguration.getMemtableEntriesMax(), 
				scalephantConfiguration.getMemtableSizeMax());
		
		memtable.acquire();
		memtable.init();
		
		tupleStoreInstances.activateNewMemtable(memtable);
	}

	/**
	 * Start the checkpoint thread if needed
	 */
	protected void startCheckpointThread() {
		if(scalephantConfiguration.getStorageCheckpointInterval() > 0) {
			final int maxUncheckpointedSeconds = scalephantConfiguration.getStorageCheckpointInterval();
			final SSTableCheckpointThread ssTableCheckpointThread = new SSTableCheckpointThread(maxUncheckpointedSeconds, this);
			final Thread checkpointThread = new Thread(ssTableCheckpointThread);
			checkpointThread.setName("Checkpoint thread for: " + sstablename.getFullname());
			checkpointThread.start();
			runningThreads.put(CHECKPOINT_THREAD, checkpointThread);
			stoppableTasks.put(CHECKPOINT_THREAD, ssTableCheckpointThread);
		} else {
			logger.info("NOT starting the checkpoint thread for: " + sstablename.getFullname());
		}
	}
	
	/**
	 * Start the compact thread if needed
	 */
	protected void startCompactThread() {
		if(scalephantConfiguration.isStorageRunCompactThread()) {
			final Thread compactThread = new Thread(new SSTableCompactorThread(this));
			compactThread.setName("Compact thread for: " + sstablename.getFullname());
			compactThread.start();
			runningThreads.put(COMPACT_THREAD, compactThread);
		} else {
			logger.info("NOT starting the sstable compact thread for: " + sstablename.getFullname());
		}
	}

	/**
	 * Start the memtable flush thread if needed
	 */
	protected void startMemtableFlushThread() {
		if(scalephantConfiguration.isStorageRunMemtableFlushThread()) {
			final MemtableFlushThread memtableFlushThread = new MemtableFlushThread(this);
			final Thread flushThread = new Thread(memtableFlushThread);
			flushThread.setName("Memtable flush thread for: " + sstablename.getFullname());
			flushThread.start();
			runningThreads.put(MEMTABLE_FLUSH_THREAD, flushThread);
			stoppableTasks.put(MEMTABLE_FLUSH_THREAD, memtableFlushThread);
		} else {
			logger.info("NOT starting the memtable flush thread for:" + sstablename.getFullname());
		}
	}

	/**
	 * Shutdown the instance
	 */
	@Override
	public void shutdown() {
		logger.info("Shuting down the instance for table: " + sstablename.getFullname());
		
		// Set ready to false. The threads will shutdown after completing
		// the running tasks
		ready = false;
		storageState.setReady(false);
		
		if(tupleStoreInstances.getMemtable() != null) {
			tupleStoreInstances.getMemtable().shutdown();
			
			// Flush in memory data
			try {
				flushMemtable();
			} catch (StorageManagerException e) {
				logger.warn("Got exception while flushing pending memtable to disk", e);
			}
		}
		
		stopThreads();

		// Close all sstables
		for(final SSTableFacade facade : tupleStoreInstances.getSstableFacades()) {
			facade.shutdown();
		}
		
		tupleStoreInstances.clear();
	}

	/**
	 * Shutdown all running service threads
	 */
	public void stopThreads() {
		
		// Stop the running tasks
		for(final Stoppable stoppable : stoppableTasks.values()) {
			stoppable.stop();
		}
		
		// Stop the corresponsing threads
		for(final Thread thread : runningThreads.values()) {
			logger.info("Interrupt and join thread: " + thread.getName());
			thread.interrupt();
			
			try {
				thread.join(THREAD_WAIT_TIMEOUT);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.warn("Got exception while waiting on thread join: " + thread.getName(), e);
			}
		}
		
		stoppableTasks.clear();
		runningThreads.clear();
	}
	
	/**
	 * Is the shutdown complete?
	 * 
	 * @return
	 */
	public boolean isShutdownComplete() {
		return (stoppableTasks.isEmpty() && runningThreads.isEmpty());
	}
	
	/**
	 * Ensure that the directory for the given table exists
	 * 
	 */
	protected void createSSTableDirIfNeeded() {
		final File rootDir = new File(scalephantConfiguration.getDataDirectory());		
		final File directoryHandle = new File(SSTableHelper.getSSTableDir(scalephantConfiguration.getDataDirectory(), sstablename.getFullname()));
		
		if(rootDir.exists() && ! directoryHandle.exists()) {
			logger.info("Create a new dir for table: " + getSSTableName());
			directoryHandle.mkdir();
		}
	}
	
	/**
	 * Scan the database directory for all existing SSTables and
	 * create reader objects
	 * @throws StorageManagerException 
	 * 
	 */
	protected void scanForExistingTables() throws StorageManagerException {
		logger.info("Scan for existing SSTables: " + getSSTableName());
		final File directoryHandle = new File(SSTableHelper.getSSTableDir(scalephantConfiguration.getDataDirectory(), sstablename.getFullname()));
		
	    checkSSTableDir(directoryHandle);
	
		final File[] entries = directoryHandle.listFiles();
				
		for(final File file : entries) {
			final String filename = file.getName();
			if(isFileNameSSTable(filename)) {
				logger.info("Found sstable: " + filename);
				
				try {
					final int sequenceNumber = SSTableHelper.extractSequenceFromFilename(sstablename, filename);
					final SSTableFacade facade = new SSTableFacade(scalephantConfiguration.getDataDirectory(), sstablename, sequenceNumber);
					facade.init();
					tupleStoreInstances.addNewDetectedSSTable(facade);
				} catch(StorageManagerException e) {
					logger.warn("Unable to parse sequence number, ignoring file: " + filename, e);
				}
			}
		}
	}
	
	/**
	 * Get the highest sequence number, based on the reader
	 * instances
	 * 
	 * @return the sequence number
	 */
	protected int getLastSequencenumberFromReader() {
		
		int number = 0;
		
		for(final SSTableFacade facade : tupleStoreInstances.getSstableFacades()) {
			final int sequenceNumber = facade.getTablebumber();
			
			if(sequenceNumber >= number) {
				number = sequenceNumber + 1;
			}
		}
		
		return number;
	}

	/**
	 * Ensure that the storage directory does exist
	 * 
	 * @param directoryHandle
	 * @throws StorageManagerException 
	 */
	public void checkSSTableDir(final File directoryHandle) throws StorageManagerException {
		if(! directoryHandle.isDirectory()) {
			final String message = "Storage directory is not an directory: " + directoryHandle;
			storageState.setReady(false);
			logger.error(message);
			throw new StorageManagerException(message);
		}		
	}
	
	/**
	 * Delete all existing data
	 * 
	 * 1) Reject new writes to this table 
	 * 2) Clear the memtable
	 * 3) Shutdown the sstable flush service
	 * 4) Wait for shutdown complete
	 * 5) Delete all persistent sstables
	 * 
	 * @return Directory was deleted or not
	 * @throws StorageManagerException 
	 */
	public boolean deleteExistingTables() throws StorageManagerException {
		logger.info("Delete all existing SSTables for relation: {}", getSSTableName());
		
		// Reject new writes
		shutdown();
		
		// Wait for in memory data flush
		while(! isShutdownComplete()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			}
		}
		
		if(getMemtable() != null && ! getMemtable().isEmpty()) {
			logger.warn("Memtable is not empty after shutdown()");
			getMemtable().clear();
		}
		
		if(! tupleStoreInstances.getMemtablesToFlush().isEmpty() ) {
			logger.warn("There are unflushed memtables after shutdown(): {}", tupleStoreInstances.getMemtablesToFlush());
		}
		
		return deletePersistentTableData();
	}

	/**
	 * Delete the persistent data of the table
	 * @return
	 */
	protected boolean deletePersistentTableData() {
		final File directoryHandle = new File(SSTableHelper.getSSTableDir(scalephantConfiguration.getDataDirectory(), sstablename.getFullname()));
	
		// Does the directory exist?
		if(! directoryHandle.isDirectory()) {
			return true;
		}

		final File[] entries = directoryHandle.listFiles();
				
		for(final File file : entries) {
			final String filename = file.getName();
			if(isFileNameSSTable(filename)) {
				logger.info("Deleting file: {} ", file);
				file.delete();
			} else if(isFileNameSSTableIndex(filename)) {
				logger.info("Deleting index file: {} ", file);
				file.delete();
			} else if(isFileNameSSTableBloomFilter(filename)) {
				logger.info("Deleting bloom filter file: {} ", file);
				file.delete();
			} else if(isFileNameSSTableMetadata(filename)) {
				logger.info("Deleting meta file: {}", file);
				file.delete();
			}
		}
		
		// Delete the directory if empty
		if(directoryHandle.listFiles().length != 0) {
			logger.info("SStable directory is not empty, skip directory delete");
			return false;
		} else {
			directoryHandle.delete();
			return true;
		}
	}

	/**
	 * Belongs the given filename to a SSTable?
	 * 
	 * @param filename
	 * @return
	 */
	protected boolean isFileNameSSTable(final String filename) {
		return filename.startsWith(SSTableConst.SST_FILE_PREFIX) 
				&& filename.endsWith(SSTableConst.SST_FILE_SUFFIX);
	}
	
	/**
	 * Belongs the given filename to a SSTable index?
	 * 
	 * @param filename
	 * @return
	 */
	protected boolean isFileNameSSTableIndex(final String filename) {
		return filename.startsWith(SSTableConst.SST_FILE_PREFIX) 
				&& filename.endsWith(SSTableConst.SST_INDEX_SUFFIX);
	}
	
	/**
	 * Belongs the given filename to a SSTable bloom filter file?
	 * @param filename
	 * @return
	 */
	protected boolean isFileNameSSTableBloomFilter(final String filename) {
		return filename.startsWith(SSTableConst.SST_FILE_PREFIX) 
				&& filename.endsWith(SSTableConst.SST_BLOOM_SUFFIX);
	}
	
	/**
	 * Belongs the given filename to a SSTable meta file?
	 * @param filename
	 * @return
	 */
	protected boolean isFileNameSSTableMetadata(final String filename) {
		return filename.startsWith(SSTableConst.SST_FILE_PREFIX) 
				&& filename.endsWith(SSTableConst.SST_META_SUFFIX);
	}
		
	/**
	 * Search for the most recent version of the tuple
	 * @param key
	 * @return The tuple or null
	 * @throws StorageManagerException
	 */
	public Tuple get(final String key) throws StorageManagerException {
			
		if(! storageState.isReady()) {
			throw new StorageManagerException("Storage manager is not ready");
		}
		
		Tuple mostRecentTuple = null;
		boolean readComplete = false;
		while(! readComplete) {
			readComplete = true;
			
			// Read data from the persistent SSTables
			for(final ReadOnlyTupleStorage facade : getTupleStoreInstances().getAllTupleStorages()) {
				
				final boolean canBeUsed = facade.acquire();
				
				if(! canBeUsed) {
					readComplete = false;
					break;
				} else {
					try {
						if(canStorageContainNewerTuple(mostRecentTuple, facade)) {
							final Tuple facadeTuple = facade.get(key);
							mostRecentTuple = TupleHelper.returnMostRecentTuple(mostRecentTuple, facadeTuple);
						}
					} catch (Exception e) {
						throw e;
					} finally {
						facade.release();
					}
				} 
			}
		}
		
		return TupleHelper.replaceDeletedTupleWithNull(mostRecentTuple);
	}	

	
	/**
	 * Can the given storage contain a newer tuple than the recent tuple?
	 * @param memtable
	 * @return
	 */
	protected boolean canStorageContainNewerTuple(final Tuple mostRecentTuple, final ReadOnlyTupleStorage memtable) {
		if(mostRecentTuple == null) {
			return true;
		}
		
		if(memtable.getNewestTupleTimestamp() > mostRecentTuple.getTimestamp()) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * Get all tuples that match the predicate
	 */
	public CloseableIterator<Tuple> getMatchingTuples(final Predicate predicate) {
		final SSTableQueryProcessor ssTableQueryProcessor = new SSTableQueryProcessor(predicate, this);
		return ssTableQueryProcessor.iterator();
	}
	
	/**
	 * Get and increase the table number
	 * @return
	 */
	public int increaseTableNumber() {
		return tableNumber.getAndIncrement();
	}

	/**
	 * Is the instance ready?
	 * @return
	 */
	public boolean isReady() {
		return ready;
	}

	/**
	 * Set ready flag
	 * @param ready
	 */
	public void setReady(final boolean ready) {
		this.ready = ready;
	}

	/**
	 * Get the sstable name for this instance
	 * @return
	 */
	public SSTableName getSSTableName() {
		return sstablename;
	}

	/**
	 * Returns the configuration
	 * @return
	 */
	public ScalephantConfiguration getScalephantConfiguration() {
		return scalephantConfiguration;
	}

	/**
	 * Get the name of this service
	 */
	@Override
	public String getServicename() {
		return "SSTable manager";
	}

	/**
	 * Flush the open memtable to disk
	 * @throws StorageManagerException
	 */
	public void flushMemtable() throws StorageManagerException {
		initNewMemtable();
	}

	// These methods are required by the interface
	public void put(final Tuple tuple) throws StorageManagerException {
		if(! storageState.isReady()) {
			throw new StorageManagerException("Storage manager is not ready");
		}
		
		// Ensure that only one memtable is newly created
		synchronized (this) {	
			if(getMemtable().isFull()) {
				flushMemtable();
			}
			
			getMemtable().put(tuple);
		}
	}

	public void delete(final String key, final long timestamp) throws StorageManagerException {
		if(! storageState.isReady()) {
			throw new StorageManagerException("Storage manager is not ready");
		}
		
		// Ensure that only one memtable is newly created
		synchronized (this) {	
			if(getMemtable().isFull()) {
				flushMemtable();
			}
			
			getMemtable().delete(key, timestamp);
		}
	}

	public void clear() throws StorageManagerException {
		deleteExistingTables();
		init();
	}

	/**
	 * Get the tuple storate instance manager
	 * @return
	 */
	public TupleStoreInstances getTupleStoreInstances() {
		return tupleStoreInstances;
	}
	
	/**
	 * Get the active memtable
	 * @return
	 */
	public Memtable getMemtable() {
		return tupleStoreInstances.getMemtable();
	}
}
