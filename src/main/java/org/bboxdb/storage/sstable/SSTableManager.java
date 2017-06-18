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
import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.DistributionGroupMetadata;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.memtable.Memtable;
import org.bboxdb.storage.memtable.MemtableAndSSTableManager;
import org.bboxdb.storage.memtable.Storage;
import org.bboxdb.storage.sstable.compact.SSTableCompactorThread;
import org.bboxdb.storage.sstable.reader.SSTableFacade;
import org.bboxdb.util.ServiceState;
import org.bboxdb.util.ServiceState.State;
import org.bboxdb.util.ThreadHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableManager implements BBoxDBService {
		
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
	protected ServiceState serviceState;
	
	/**
	 * The running threads
	 */
	protected final List<Thread> runningThreads;

	/**
	 * The SSTable compactor
	 */
	protected SSTableCompactorThread sstableCompactor;
	
	/**
	 * The state (read only / read write) of the manager
	 */
	protected volatile SSTableManagerState sstableManagerState;
	
	/**
	 * The storage
	 */
	protected final Storage storage;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableManager.class);

	public SSTableManager(final Storage storage, final SSTableName sstablename, 
			final BBoxDBConfiguration configuration) {
		
		this.storage = storage;
		this.configuration = configuration;
		this.sstablename = sstablename;
		this.tableNumber = new AtomicInteger();
		this.runningThreads = new ArrayList<>();
		this.sstableCompactor = null;
		this.sstableManagerState = SSTableManagerState.READ_WRITE;
		this.tupleStoreInstances = new TupleStoreInstanceManager();
		
		// Close open ressources when the failed state is entered
		this.serviceState = new ServiceState(); 
		serviceState.registerCallback((s) -> {
			if(s.getState() == State.FAILED) {
				closeRessources();
			}
		});
	}

	/**
	 * Init the instance
	 * 
	 */
	@Override
	public void init() {
		
		if(! serviceState.isInNewState()) {
			logger.warn("SSTable manager state is not new init() is called: {}", serviceState.getState());
			return;
		}
		
		serviceState.dipatchToStarting();
		
		try {
			logger.info("Init a new instance for the table: {}", sstablename.getFullname());
			
			tupleStoreInstances.clear();
			runningThreads.clear();

			createSSTableDirIfNeeded();
			initNewMemtable();
			scanForExistingTables();
			
			tableNumber.set(getLastSequencenumberFromReader() + 1);

			sstableManagerState = SSTableManagerState.READ_WRITE;
			
			// Set to ready before the threads are started
			serviceState.dispatchToRunning();

			startCompactThread();
			startCheckpointThread();
		} catch (StorageManagerException e) {
			logger.error("Unable to init the instance: " +  sstablename.getFullname(), e);
			serviceState.dispatchToFailed(e);
		}
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
	 * Shutdown the instance
	 */
	@Override
	public void shutdown() {
		logger.info("Shuting down the instance for table: {}", sstablename.getFullname());
		
		if(! serviceState.isInRunningState()) {
			logger.error("Shutdown called but state is not running: " + serviceState.getState());
			return;
		}
		
		// Set ready to false and reject write requests
		serviceState.dispatchToStopping();
		flush();
		
		try {
			tupleStoreInstances.waitForAllMemtablesFlushed();
		} catch (InterruptedException e) {
			logger.debug("Wait for memtable flush interrupted");
			Thread.currentThread().interrupt();
			return;
		}
		
		closeRessources();
		
		serviceState.dispatchToTerminated();
	}

	/**
	 * Close all open Ressources
	 */
	protected void closeRessources() {
		stopThreads();
		
		if(tupleStoreInstances != null) {
			tupleStoreInstances.getSstableFacades().forEach(f -> f.shutdown());		
			tupleStoreInstances.clear();
		}
	}

	/**
	 * Flush all in memory data, if the memtable flush thread is running
	 * @return 
	 */
	public boolean flush() {
		final Memtable activeMemtable = tupleStoreInstances.getMemtable();
		
		if(activeMemtable != null) {
			// Flush in memory data	
			initNewMemtable();
			
			try {
				tupleStoreInstances.waitForMemtableFlush(activeMemtable);
			} catch (InterruptedException e) {
				logger.info("Got interrupted exception while waiting for memtable flush");
				Thread.currentThread().interrupt();
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Wait for the shutdown to complete
	 * @throws InterruptedException 
	 */
	public void awaitShutdown() throws InterruptedException {
		serviceState.awaitTerminatedOrFailed();
	}

	/**
	 * Shutdown all running service threads
	 */
	public void stopThreads() {

		// Set the flush mode to memory only
		sstableManagerState = SSTableManagerState.READ_ONLY;
		
		logger.info("Stop running threads");
		ThreadHelper.stopThreads(runningThreads);
		
		// Complete shutdown by clearing running threads
		runningThreads.clear();
	}
	
	/**
	 * Is the shutdown complete?
	 * @return
	 */
	public boolean isShutdownComplete() {
		return serviceState.isInFinishedState();
	}
	
	/**
	 * Ensure that the directory for the given table exists
	 * @throws StorageManagerException 
	 * 
	 */
	protected void createSSTableDirIfNeeded() throws StorageManagerException {
		final String storageDir = storage.getBasedir().getAbsolutePath();
		final String dgroupDir = SSTableHelper.getDistributionGroupDir(storageDir, sstablename);
		final File dgroupDirHandle = new File(dgroupDir);
				
		if(! dgroupDirHandle.exists()) {
			logger.info("Create a new directory for dgroup: {}", dgroupDir);
			dgroupDirHandle.mkdir();	
			try {
				writeMetaData();
			} catch (Exception e) {
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
		
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
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
		final String storageDir = storage.getBasedir().getAbsolutePath();
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
				} catch(Exception e) {
					logger.warn("Unable to load file: " + filename, e);
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
			final StorageManagerException exception = new StorageManagerException(message);
			serviceState.dispatchToFailed(exception);
			logger.error(message);
			throw exception;
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
			
		if(! serviceState.isInRunningState()) {
			throw new StorageManagerException("Storage manager is not ready: " 
					+ sstablename.getFullname() 
					+ " state: " + serviceState);
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
					if(execution == Const.OPERATION_RETRY - 1) {
						logger.error("Unable to aquire: {} with {} retries", tupleStorage, execution);
					}
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
	 * Open a new memtable and schedule the old memtable for flushing
	 * @throws StorageManagerException
	 */
	public synchronized void initNewMemtable() {
		final Memtable memtable = new Memtable(sstablename, 
				configuration.getMemtableEntriesMax(), 
				configuration.getMemtableSizeMax());
		
		memtable.acquire();
		memtable.init();
		
		final Memtable oldMemtable = tupleStoreInstances.activateNewMemtable(memtable);	
		
		final MemtableAndSSTableManager memtableTask = new MemtableAndSSTableManager(oldMemtable, this);
		storage.scheduleMemtableFlush(memtableTask);
		
		logger.debug("Activated a new memtable: {}", memtable.getInternalName());
	}

	/**
	 * Store a new tuple
	 * @param tuple
	 * @throws StorageManagerException
	 */
	public void put(final Tuple tuple) throws StorageManagerException {
		
		if(! serviceState.isInRunningState()) {
			throw new StorageManagerException("Storage manager is not ready: " 
					+ sstablename.getFullname() 
					+ " state: " + serviceState);
		}
		
		try {
			// Ensure that only one memtable is newly created
			synchronized (this) {	
				if(getMemtable().isFull()) {
					initNewMemtable();
				}
				
				getMemtable().put(tuple);
			}
		} catch (StorageManagerException e) {
			serviceState.dispatchToFailed(e);
			throw e;
		}
	}

	/**
	 * Delete the given tuple
	 * @param key
	 * @param timestamp
	 * @throws StorageManagerException
	 */
	public void delete(final String key, final long timestamp) throws StorageManagerException {
		if(! serviceState.isInRunningState()) {
			throw new StorageManagerException("Storage manager is not ready: " 
					+ sstablename.getFullname() 
					+ " state: " + serviceState);
		}
		
		// Ensure that only one memtable is newly created
		try {
			synchronized (this) {	
				if(getMemtable().isFull()) {
					initNewMemtable();
				}
				
				getMemtable().delete(key, timestamp);
			}
		} catch (StorageManagerException e) {
			serviceState.dispatchToFailed(e);
			throw e;
		}
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
	
	/**
	 * Get the service state
	 * @return
	 */
	public ServiceState getServiceState() {
		return serviceState;
	}

	/**
	 * Get the manager state
	 * @return
	 */
	public SSTableManagerState getSstableManagerState() {
		return sstableManagerState;
	}

	/**
	 * Set the manager state
	 * @param sstableManagerState
	 */
	public void setSstableManagerState(final SSTableManagerState sstableManagerState) {
		this.sstableManagerState = sstableManagerState;
	}
}
