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
package org.bboxdb.storage.registry;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
import org.bboxdb.storage.sstable.SSTableHelper;
import org.bboxdb.storage.sstable.TupleHelper;
import org.bboxdb.storage.sstable.reader.SSTableFacade;
import org.bboxdb.util.RejectedException;
import org.bboxdb.util.ServiceState;
import org.bboxdb.util.ServiceState.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleStoreManager implements BBoxDBService {
		
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
	 * The number of the next SSTable name
	 */
	protected AtomicInteger nextFreeTableNumber;
	
	/**
	 * The corresponding storage manager state
	 */
	protected ServiceState serviceState;

	/**
	 * The storage
	 */
	protected final DiskStorage storage;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleStoreManager.class);

	public TupleStoreManager(final DiskStorage storage, final SSTableName sstablename, 
			final BBoxDBConfiguration configuration) {
		
		this.storage = storage;
		this.configuration = configuration;
		this.sstablename = sstablename;
		this.nextFreeTableNumber = new AtomicInteger();
		this.tupleStoreInstances = new TupleStoreInstanceManager();
		
		// Close open resources when the failed state is entered
		this.serviceState = new ServiceState(); 
		serviceState.registerCallback((s) -> {
			if(s.getState() == State.FAILED) {
				closeRessources();
			}
		});
	}

	/**
	 * Init the instance
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

			createSSTableDirIfNeeded();
			initNewMemtable();
			scanForExistingTables();
			
			nextFreeTableNumber.set(getLastSequencenumberFromReader() + 1);
			tupleStoreInstances.setReadWrite();
			
			// Set to ready before the threads are started
			serviceState.dispatchToRunning();
		} catch (StorageManagerException e) {
			logger.error("Unable to init the instance: " +  sstablename.getFullname(), e);
			serviceState.dispatchToFailed(e);
		}
	}

	/**
	 * Shutdown the instance
	 */
	@Override
	public void shutdown() {
		logger.info("Shuting down the instance for table: {}", sstablename.getFullname());
		
		if(! serviceState.isInRunningState()) {
			logger.error("Shutdown called but state is not running: {}", serviceState.getState());
			return;
		}
		
		// Set service state to stopping
		serviceState.dispatchToStopping();
		
		// Flush only when memtable is in RW state, otherwise the memtable flush callbacks
		// could not be processed
		if(tupleStoreInstances.getState() == TupleStoreManagerState.READ_WRITE) {
			try {
				logger.info("Flushing tables for shutdown");
				flush();
				tupleStoreInstances.waitForAllMemtablesFlushed();
			} catch (InterruptedException e) {
				logger.debug("Wait for memtable flush interrupted");
				Thread.currentThread().interrupt();
				return;
			}
		} else {
			logger.info("NOT flushing tables for shutdown");
		}
		
		closeRessources();
		
		serviceState.dispatchToTerminated();
	}

	/**
	 * Close all open Resources
	 */
	protected void closeRessources() {
		setToReadOnly();
		
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
	 * Set the mode to read only
	 */
	public void setToReadOnly() {
		// Set the flush mode to read only
		tupleStoreInstances.setReadOnly();
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
			logger.info("Create a new directory for dgroup {} ({})", 
					sstablename.getDistributionGroup(), 
					dgroupDir);
			
			final boolean mkdirResult = dgroupDirHandle.mkdirs();
			
			assert (mkdirResult == true) : "Unable to create dir: " + dgroupDirHandle;
			
			try {
				writeMetaData();
			} catch (Exception e) {
				logger.error("Unable to write meta data", e);
			}
		}
		
		final String ssTableDir = SSTableHelper.getSSTableDir(storageDir, sstablename);
		final File ssTableDirHandle = new File(ssTableDir);

		if(! ssTableDirHandle.exists()) {
			logger.info("Create a new dir for table {} ({}) ", sstablename.getFullname(), ssTableDirHandle);
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
		
		DistributionGroupMetadataHelper.writeMedatadataForGroup(storage.getBasedir().getAbsolutePath(), 
				sstablename.getDistributionGroupObject(), 
				distributionGroupMetadata);
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
					final SSTableFacade facade = new SSTableFacade(storageDir, sstablename, sequenceNumber, 
							configuration.getSstableKeyCacheEntries());
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
		return nextFreeTableNumber.getAndIncrement();
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
		
		final MemtableAndSSTableManagerPair memtableTask = new MemtableAndSSTableManagerPair(oldMemtable, this);
		storage.scheduleMemtableFlush(memtableTask);
		
		logger.debug("Activated a new memtable: {}", memtable.getInternalName());
	}

	/**
	 * Store a new tuple
	 * @param tuple
	 * @throws StorageManagerException
	 * @throws RejectedException 
	 */
	public void put(final Tuple tuple) throws StorageManagerException, RejectedException {
		
		if(! serviceState.isInRunningState()) {
			throw new StorageManagerException("Storage manager is not ready: " 
					+ sstablename.getFullname() 
					+ " state: " + serviceState);
		}
		
		if(tupleStoreInstances.getState() == TupleStoreManagerState.READ_ONLY) {
			throw new RejectedException("Storage manager is in read only state");
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
	 * @throws RejectedException 
	 */
	public void delete(final String key, final long timestamp) throws StorageManagerException, RejectedException {
		if(! serviceState.isInRunningState()) {
			throw new StorageManagerException("Storage manager is not ready: " 
					+ sstablename.getFullname() 
					+ " state: " + serviceState);
		}
		
		if(tupleStoreInstances.getState() == TupleStoreManagerState.READ_ONLY) {
			throw new RejectedException("Storage manager is in read only state");
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
	 * Replace memtable delegate
	 * @param memtable
	 * @param sstableFacade
	 * @throws RejectedException 
	 */
	public void replaceMemtableWithSSTable(final Memtable memtable, final SSTableFacade sstableFacade) 
			throws RejectedException {

		if(tupleStoreInstances.getState() == TupleStoreManagerState.READ_ONLY) {
			throw new RejectedException("Storage manager is in read only state");
		}
		
		tupleStoreInstances.replaceMemtableWithSSTable(memtable, sstableFacade);
	}

	/**
	 * Replace sstables delegate
	 * @param newFacedes
	 * @param oldFacades
	 * @throws RejectedException 
	 */
	public void replaceCompactedSStables(final List<SSTableFacade> newFacedes, 
			final List<SSTableFacade> oldFacades) throws RejectedException {
		
		if(tupleStoreInstances.getState() == TupleStoreManagerState.READ_ONLY) {
			throw new RejectedException("Storage manager is in read only state");
		}
		
		tupleStoreInstances.replaceCompactedSStables(newFacedes, oldFacades);
	}

	/**
	 * Get all sstable facades
	 * @return
	 */
	public Collection<SSTableFacade> getSstableFacades() {
		return tupleStoreInstances.getSstableFacades();
	}

	/**
	 * In memory delegate
	 * @return
	 */
	public List<ReadOnlyTupleStorage> getAllInMemoryStorages() {
		return tupleStoreInstances.getAllInMemoryStorages();
	}
	
	/**
	 * Get all tuple storages delegate
	 * @return
	 */
	public List<ReadOnlyTupleStorage> getAllTupleStorages() {
		return tupleStoreInstances.getAllTupleStorages();
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
	public TupleStoreManagerState getSstableManagerState() {
		return tupleStoreInstances.getState();
	}

}
