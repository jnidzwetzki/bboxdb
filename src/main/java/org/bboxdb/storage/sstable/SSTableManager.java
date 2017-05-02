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
package org.bboxdb.storage.sstable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.bboxdb.distribution.DistributionGroupMetadataHelper;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.misc.Const;
import org.bboxdb.storage.Memtable;
import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.DistributionGroupMetadata;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.compact.SSTableCompactorThread;
import org.bboxdb.storage.sstable.reader.SSTableFacade;
import org.bboxdb.util.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableManager implements BBoxDBService {
	
	/**
	 * The directory where this table is stored
	 */
	protected String storageDir;
	
	/**
	 * The name of the table
	 */
	protected final SSTableName sstablename;
	
	/**
	 * The tuple store instances
	 */
	protected final TupleStoreInstanceManager tupleStoreInstances;
	
	/**
	 * The Storage configuration
	 */
	protected final BBoxDBConfiguration configuration;
	
	/**
	 * The number of the table
	 */
	protected AtomicInteger tableNumber;
	
	/**
	 * The corresponding storage manager state
	 */
	protected State storageState;
	
	/**
	 * The running threads
	 */
	protected final List<Thread> runningThreads;
	
	/**
	 * The timeout for a thread join (10 seconds)
	 */
	protected long THREAD_WAIT_TIMEOUT = TimeUnit.SECONDS.toMillis(10);
	
	/**
	 * The SSTable compactor
	 */
	protected SSTableCompactorThread sstableCompactor;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableManager.class);

	public SSTableManager(final String storageDir, final SSTableName sstablename, 
			final BBoxDBConfiguration configuration) {
		
		this.storageDir = storageDir;
		this.configuration = configuration;
		this.storageState = new State(false); 
		this.sstablename = sstablename;
		this.tableNumber = new AtomicInteger();
		
		this.tupleStoreInstances = new TupleStoreInstanceManager();
		this.runningThreads = new ArrayList<>();
		this.sstableCompactor = null;
	}

	/**
	 * Init the instance
	 * 
	 */
	@Override
	public void init() {
		
		if(storageState.isReady()) {
			logger.warn("SSTable manager is active and init() is called");
			return;
		}
		
		logger.info("Init a new instance for the table: {}", sstablename.getFullname());
		
		tupleStoreInstances.clear();
		runningThreads.clear();
		
		try {
			createSSTableDirIfNeeded();
			flushAndInitMemtable();
			scanForExistingTables();
		} catch (StorageManagerException e) {
			logger.error("Unable to init the instance: " +  sstablename.getFullname(), e);
			return;
		}
		
		tableNumber.set(getLastSequencenumberFromReader() + 1);

		// Set to ready before the threads are started
		storageState.setReady(true);

		startMemtableFlushThread();
		startCompactThread();
		startCheckpointThread();
	}

	/**
	 * Start the checkpoint thread if needed
	 */
	protected void startCheckpointThread() {
		if(configuration.getStorageCheckpointInterval() > 0) {
			final int maxUncheckpointedSeconds = configuration.getStorageCheckpointInterval();
			final SSTableCheckpointThread ssTableCheckpointThread = new SSTableCheckpointThread(maxUncheckpointedSeconds, this);
			final Thread checkpointThread = new Thread(ssTableCheckpointThread);
			checkpointThread.setName("Checkpoint thread for: " + sstablename.getFullname());
			checkpointThread.start();
			runningThreads.add(checkpointThread);
		} else {
			logger.info("NOT starting the checkpoint thread for: " + sstablename.getFullname());
		}
	}
	
	/**
	 * Start the compact thread if needed
	 */
	protected void startCompactThread() {
		if(configuration.isStorageRunCompactThread()) {
			sstableCompactor = new SSTableCompactorThread(this);
			final Thread compactThread = new Thread(sstableCompactor);
			compactThread.setName("Compact thread for: " + sstablename.getFullname());
			compactThread.start();
			runningThreads.add(compactThread);
		} else {
			logger.info("NOT starting the sstable compact thread for: " + sstablename.getFullname());
		}
	}

	/**
	 * Start the memtable flush thread if needed
	 */
	protected void startMemtableFlushThread() {
		if(configuration.isStorageRunMemtableFlushThread()) {
			final MemtableFlushThread memtableFlushThread = new MemtableFlushThread(this);
			final Thread flushThread = new Thread(memtableFlushThread);
			flushThread.setName("Memtable flush thread for: " + sstablename.getFullname());
			flushThread.start();
			runningThreads.add(flushThread);
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
		
		// Set ready to false and reject write requests
		storageState.setReady(false);
		
		if(tupleStoreInstances.getMemtable() != null) {
			tupleStoreInstances.getMemtable().shutdown();
			
			// Flush in memory data	
			flushAndInitMemtable();
		}
		
		stopThreads();

		// Close all sstables
		for(final SSTableFacade facade : tupleStoreInstances.getSstableFacades()) {
			facade.shutdown();
		}
		
		tupleStoreInstances.clear();
	}
	
	/**
	 * Wait for the shutdown to complete
	 */
	public void waitForShutdownToComplete() {
		
		if(storageState.isReady()) {
			throw new IllegalStateException("waitForShutdownToComplete called but no shutdown is active");
		}
		
		final Queue<Memtable> memtablesToFlush = tupleStoreInstances.getMemtablesToFlush();
		
		// New writes are rejected, so we wait for the unflushed memtables
		// to get flushed..
		while(! memtablesToFlush.isEmpty()) {
			try {
				memtablesToFlush.wait();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
		
		if(getMemtable() != null && ! getMemtable().isEmpty()) {
			logger.warn("Memtable is not empty after shutdown().");
		}
		
		if(! memtablesToFlush.isEmpty() ) {
			logger.warn("There are unflushed memtables after shutdown(): {}",  memtablesToFlush);
		}
	}


	/**
	 * Shutdown all running service threads
	 */
	public void stopThreads() {

		// Stop the corresponding threads
		for(final Thread thread : runningThreads) {
			logger.info("Interrupt thread: {}", thread.getName());
			thread.interrupt();
		}
		
		// Join threads
		for(final Thread thread : runningThreads) {
			try {
				logger.info("Join thread: {}", thread.getName());

				thread.join(THREAD_WAIT_TIMEOUT);
				
				// Is the thread still alive?
				if(thread.isAlive()) {
					logger.error("Unable to stop thread: {}", thread.getName());
				}
				
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.warn("Got exception while waiting on thread join: " + thread.getName(), e);
			}
		}
		
		runningThreads.clear();
	}
	
	/**
	 * Is the shutdown complete?
	 * 
	 * @return
	 */
	public boolean isShutdownComplete() {
		
		if(storageState.isReady()) {
			return false;
		}
		
		return (runningThreads.isEmpty());
	}
	
	/**
	 * Ensure that the directory for the given table exists
	 * @throws StorageManagerException 
	 * 
	 */
	protected void createSSTableDirIfNeeded() throws StorageManagerException {
		final String dgroupDir = SSTableHelper.getDistributionGroupDir(storageDir, sstablename);
		final File dgroupDirHandle = new File(dgroupDir);
				
		if(! dgroupDirHandle.exists()) {
			logger.info("Create a new dir for dgroup: " + dgroupDir);
			dgroupDirHandle.mkdir();	
			try {
				writeMetaData();
			} catch (ZookeeperException | ZookeeperNotFoundException | IOException e) {
				logger.error("Unable to write meta data", e);
			}
		}
		
		final String ssTableDir = SSTableHelper.getSSTableDir(storageDir, sstablename);
		final File ssTableDirHandle = new File(ssTableDir);

		if(! ssTableDirHandle.exists()) {
			logger.info("Create a new dir for table: " + sstablename.getFullname());
			ssTableDirHandle.mkdir();
		}
	}

	/**
	 * Write the meta data for the distribution group
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 * @throws IOException 
	 */
	protected void writeMetaData() throws ZookeeperException, ZookeeperNotFoundException, IOException {
		
		if(! sstablename.isDistributedTable()) {	
			return;
		}
		
		logger.debug("Write meta data for distribution group: ", sstablename.getDistributionGroup());
		
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClientAndInit();
		final DistributionGroupZookeeperAdapter dAdapter = new DistributionGroupZookeeperAdapter(zookeeperClient);
		final String version = dAdapter.getVersionForDistributionGroup(sstablename.getDistributionGroup(), null);
		
		DistributionGroupMetadata distributionGroupMetadata = new DistributionGroupMetadata();
		distributionGroupMetadata.setVersion(version);
		
		DistributionGroupMetadataHelper.writeMedatadataForGroup(sstablename.getDistributionGroupObject(), distributionGroupMetadata);
	}
	
	/**
	 * Scan the database directory for all existing SSTables and
	 * create reader objects
	 * @throws StorageManagerException 
	 * 
	 */
	protected void scanForExistingTables() throws StorageManagerException {
		logger.info("Scan for existing SSTables: " + sstablename.getFullname());
		final String ssTableDir = SSTableHelper.getSSTableDir(storageDir, sstablename);
		final File directoryHandle = new File(ssTableDir);
		
	    checkSSTableDir(directoryHandle);
	
		final File[] entries = directoryHandle.listFiles();
				
		for(final File file : entries) {
			final String filename = file.getName();
			if(SSTableHelper.isFileNameSSTable(filename)) {
				logger.info("Found sstable: " + filename);
				
				try {
					final int sequenceNumber = SSTableHelper.extractSequenceFromFilename(sstablename, filename);
					final SSTableFacade facade = new SSTableFacade(storageDir, sstablename, sequenceNumber);
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
		
		return tupleStoreInstances
			.getSstableFacades()
			.stream()
			.mapToInt(f -> f.getTablebumber())
			.max()
			.orElse(0);
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
	 * Delete the persistent data of the table
	 * @return
	 */
	public static boolean deletePersistentTableData(final String dataDirectory, final SSTableName sstableName) {
		logger.info("Delete all existing SSTables for relation: {}", sstableName.getFullname());

		final File directoryHandle = new File(SSTableHelper.getSSTableDir(dataDirectory, sstableName));
	
		// Does the directory exist?
		if(! directoryHandle.isDirectory()) {
			return true;
		}

		final File[] entries = directoryHandle.listFiles();
				
		for(final File file : entries) {
			final String filename = file.getName();
			
			if(SSTableHelper.isFileNameSSTable(filename)) {
				logger.info("Deleting file: {} ", file);
				file.delete();
			} else if(SSTableHelper.isFileNameSSTableIndex(filename)) {
				logger.info("Deleting index file: {} ", file);
				file.delete();
			} else if(SSTableHelper.isFileNameSSTableBloomFilter(filename)) {
				logger.info("Deleting bloom filter file: {} ", file);
				file.delete();
			} else if(SSTableHelper.isFileNameSSTableMetadata(filename)) {
				logger.info("Deleting meta file: {}", file);
				file.delete();
			} else if(SSTableHelper.isFileNameSpatialIndex(filename)) {
				logger.info("Deleting spatial index file: {}", file);
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
	 * Search for the most recent version of the tuple
	 * @param key
	 * @return The tuple or null
	 * @throws StorageManagerException
	 */
	public Tuple get(final String key) throws StorageManagerException {
			
		if(! storageState.isReady()) {
			throw new StorageManagerException("Storage manager is not ready: " + sstablename.getFullname());
		}
		
		Tuple mostRecentTuple = null;
		final List<ReadOnlyTupleStorage> aquiredStorages = new ArrayList<ReadOnlyTupleStorage>();
		
		try {
			aquiredStorages.addAll(aquireStorage());
			
			for(final ReadOnlyTupleStorage tupleStorage : aquiredStorages) {
				if(TupleHelper.canStorageContainNewerTuple(mostRecentTuple, tupleStorage)) {
					final Tuple facadeTuple = tupleStorage.get(key);
					mostRecentTuple = TupleHelper.returnMostRecentTuple(mostRecentTuple, facadeTuple);
				}
			}
		} catch (Exception e) {
			throw e;
		} finally {
			releaseStorage(aquiredStorages);
		}
		
		return TupleHelper.replaceDeletedTupleWithNull(mostRecentTuple);
	}	
	
	/**
	 * Try to acquire all needed tables
	 * @return 
	 * @throws StorageManagerException 
	 */
	public List<ReadOnlyTupleStorage> aquireStorage() throws StorageManagerException {

		for(int execution = 0; execution < Const.OPERATION_RETRY; execution++) {
			
			final List<ReadOnlyTupleStorage> aquiredStorages = new ArrayList<>();
			final List<ReadOnlyTupleStorage> knownStorages = tupleStoreInstances.getAllTupleStorages();
						
			for(final ReadOnlyTupleStorage tupleStorage : knownStorages) {
				final boolean canBeUsed = tupleStorage.acquire();
								
				if(! canBeUsed ) {
					break;
				} else {
					aquiredStorages.add(tupleStorage);
				}
			}
			
			if(knownStorages.size() == aquiredStorages.size()) {
				return aquiredStorages;
			} else {
				// one or more storages could not be acquired
				// release storages and retry
				releaseStorage(aquiredStorages);
				
				try {
					// Wait some time and try again
					Thread.sleep(10);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				}
			}
		}
		
		final List<ReadOnlyTupleStorage> knownStorages = tupleStoreInstances.getAllTupleStorages();
		for(ReadOnlyTupleStorage storage : knownStorages) {
			System.err.println("---> " + storage.getInternalName() + " / " + storage.acquire());
		}
		
		throw new StorageManagerException("Unable to aquire all sstables in " 
				+ Const.OPERATION_RETRY + " retries");
	}

	
	/**
	 * Release all acquired tables
	 */
	public void releaseStorage(List<ReadOnlyTupleStorage> storagesToRelease) {
		for(final ReadOnlyTupleStorage storage : storagesToRelease) {
			storage.release();
		}		
	}
	
	/**
	 * Get and increase the table number
	 * @return
	 */
	public int increaseTableNumber() {
		return tableNumber.getAndIncrement();
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
	public BBoxDBConfiguration getConfiguration() {
		return configuration;
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
	public void flushAndInitMemtable() {
		final Memtable memtable = new Memtable(sstablename, 
				configuration.getMemtableEntriesMax(), 
				configuration.getMemtableSizeMax());
		
		memtable.acquire();
		memtable.init();
		
		tupleStoreInstances.activateNewMemtable(memtable);	
		
		logger.debug("Activated a new memtable: " + memtable.getInternalName());
	}

	/**
	 * Store a new tuple
	 * @param tuple
	 * @throws StorageManagerException
	 */
	public void put(final Tuple tuple) throws StorageManagerException {
		if(! storageState.isReady()) {
			throw new StorageManagerException("Storage manager is not ready: " + sstablename.getFullname());
		}
		
		// Ensure that only one memtable is newly created
		synchronized (this) {	
			if(getMemtable().isFull()) {
				flushAndInitMemtable();
			}
			
			getMemtable().put(tuple);
		}
	}

	/**
	 * Delete the given tuple
	 * @param key
	 * @param timestamp
	 * @throws StorageManagerException
	 */
	public void delete(final String key, final long timestamp) throws StorageManagerException {
		if(! storageState.isReady()) {
			throw new StorageManagerException("Storage manager is not ready: " + sstablename.getFullname());
		}
		
		// Ensure that only one memtable is newly created
		synchronized (this) {	
			if(getMemtable().isFull()) {
				flushAndInitMemtable();
			}
			
			getMemtable().delete(key, timestamp);
		}
	}

	/**
	 * Delete all transient and persistent data
	 * @throws StorageManagerException
	 */
	public void clear() throws StorageManagerException {
		shutdown();
		waitForShutdownToComplete();
		
		deletePersistentTableData(storageDir, getSSTableName());
		
		init();
	}

	/**
	 * Get the tuple storage instance manager
	 * @return
	 */
	public TupleStoreInstanceManager getTupleStoreInstances() {
		return tupleStoreInstances;
	}
	
	/**
	 * Get the active memtable
	 * @return
	 */
	public Memtable getMemtable() {
		return tupleStoreInstances.getMemtable();
	}

	/**
	 * Is the storage manager ready?
	 * @return
	 */
	public boolean isReady() {
		return storageState.isReady();
	}
	
	/**
	 * Get the size of all storages
	 * @return
	 * @throws StorageManagerException
	 */
	public long getSize() throws StorageManagerException {
		List<ReadOnlyTupleStorage> storages = null;
		
		try {
			storages = aquireStorage();
			
			return storages
					.stream()
					.mapToLong(s -> s.getSize())
					.sum();
			
		} finally {
			if(storages != null) {
				releaseStorage(storages);
			}
		}
	}
	
	/** 
	 * Force a SSTable compacation
	 */
	public boolean compactSStablesNow() {
		if(sstableCompactor == null) {
			return false;
		}
		
		sstableCompactor.execute();
		
		return true;
	}
}
