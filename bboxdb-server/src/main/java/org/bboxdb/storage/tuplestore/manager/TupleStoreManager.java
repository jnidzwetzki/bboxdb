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
package org.bboxdb.storage.tuplestore.manager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.ServiceState;
import org.bboxdb.commons.ServiceState.State;
import org.bboxdb.distribution.DistributionGroupMetadataHelper;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.misc.Const;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.DistributionGroupMetadata;
import org.bboxdb.storage.entity.MemtableAndTupleStoreManagerPair;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.memtable.Memtable;
import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.bboxdb.storage.sstable.duplicateresolver.TupleDuplicateResolverFactory;
import org.bboxdb.storage.sstable.reader.SSTableFacade;
import org.bboxdb.storage.tuplestore.DiskStorage;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.Summary;

public class TupleStoreManager implements BBoxDBService {

	/**
	 * The name of the table
	 */
	protected final TupleStoreName tupleStoreName;

	/**
	 * The tuple store instances
	 */
	protected final TupleStoreInstanceManager tupleStoreInstances;

	/**
	 * The tuple store configuration
	 */
	protected TupleStoreConfiguration tupleStoreConfiguration;

	/**
	 * The Storage configuration
	 */
	protected final BBoxDBConfiguration configuration;

	/**
	 * The number of the next SSTable name
	 */
	protected final AtomicInteger nextFreeTableNumber;

	/**
	 * The corresponding storage manager state
	 */
	protected final ServiceState serviceState;

	/**
	 * The storage
	 */
	protected final DiskStorage storage;

	/**
	 * The insert callbacks
	 */
	protected final List<Consumer<Tuple>> insertCallbacks;

	/**
	 * The get performance counter
	 */
	private final static Summary getRequestLatency = Summary.build()
			.name("bboxdb_request_get_latency_seconds")
			.help("Get request latency in seconds.").register();

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleStoreManager.class);

	public TupleStoreManager(final DiskStorage storage, final TupleStoreName sstablename, 
			final BBoxDBConfiguration configuration) {

		this.storage = storage;
		this.configuration = configuration;
		this.tupleStoreName = sstablename;
		this.nextFreeTableNumber = new AtomicInteger();
		this.tupleStoreInstances = new TupleStoreInstanceManager();
		this.insertCallbacks = new ArrayList<>();

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
			logger.info("Init a new instance for the table: {}", tupleStoreName.getFullname());

			tupleStoreInstances.clear();

			initNewMemtable();
			scanForExistingTables();
			loadTuplstoreMetaData();

			nextFreeTableNumber.set(getLastSequencenumberFromReader() + 1);
			tupleStoreInstances.setReadWrite();

			// Set to ready before the threads are started
			serviceState.dispatchToRunning();
		} catch (StorageManagerException e) {
			logger.error("Unable to init the instance: " +  tupleStoreName.getFullname(), e);
			serviceState.dispatchToFailed(e);
		}
	}

	/**
	 * Shutdown the instance
	 */
	@Override
	public void shutdown() {
		logger.info("Shuting down the instance for table: {}", tupleStoreName.getFullname());

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
		tupleStoreInstances.getSstableFacades().forEach(f -> f.shutdown());		
		tupleStoreInstances.clear();
	}

	/**
	 * Flush all in memory data, if the memtable flush thread is running
	 * @return 
	 */
	public boolean flush() {
		final Memtable activeMemtable = tupleStoreInstances.getMemtable();

		if(activeMemtable == null) {
			return true;
		}

		// Flush in memory data	
		initNewMemtable();

		try {
			tupleStoreInstances.waitForMemtableFlush(activeMemtable);
		} catch (InterruptedException e) {
			logger.info("Got interrupted exception while waiting for memtable flush");
			Thread.currentThread().interrupt();
			return false;
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
	 * Set the mode to read only
	 */
	public void setToReadWrite() {
		// Set the flush mode to read write
		tupleStoreInstances.setReadWrite();
	}
	
	/**
	 * Is the shutdown complete?
	 * @return
	 */
	public boolean isShutdownComplete() {
		return serviceState.isInFinishedState();
	}

	/**
	 * Create the given SSTable
	 * @param configuration
	 * @throws StorageManagerException 
	 */
	public void create(final TupleStoreConfiguration configuration) throws StorageManagerException {
		createSSTableDirIfNeeded();

		writeTupleStoreMetaData(configuration);
	}

	/**
	 * Write the Tuplestore meta data
	 * @param configuration
	 * @throws IOException 
	 */
	protected void writeTupleStoreMetaData(final TupleStoreConfiguration configuration) 
			throws StorageManagerException {

		final File metadataFile = getTuplestoreMetadataFile();

		assert (! metadataFile.exists()) : "Tuple store metadata file already exist: " + metadataFile;

		try {
			configuration.exportToYamlFile(metadataFile);
		} catch (IOException e) {
			throw new StorageManagerException(e);
		}
	}

	/**
	 * Load the tuplestore file
	 */
	protected void loadTuplstoreMetaData() {
		final File metadataFile = getTuplestoreMetadataFile();

		assert (metadataFile.exists()) : "Tuple store metadata file don't exist: " + metadataFile;

		tupleStoreConfiguration = TupleStoreConfiguration.importFromYamlFile(metadataFile);
	}

	/**
	 * Get the tuplestore metadata file
	 * @return
	 */
	protected File getTuplestoreMetadataFile() {
		final String storageDir = storage.getBasedir().getAbsolutePath();
		final String ssTableDir = SSTableHelper.getSSTableDir(storageDir, tupleStoreName);
		return new File(ssTableDir + File.separatorChar + SSTableConst.TUPLE_STORE_METADATA);
	}

	/**
	 * Ensure that the directory for the given table exists
	 * @throws StorageManagerException 
	 * 
	 */
	protected void createSSTableDirIfNeeded() throws StorageManagerException {
		final String storageDir = storage.getBasedir().getAbsolutePath();
		final String dgroupDir = SSTableHelper.getDistributionGroupDir(storageDir, tupleStoreName);
		final File dgroupDirHandle = new File(dgroupDir);

		if(! dgroupDirHandle.exists()) {
			logger.info("Create a new directory for dgroup {} ({})", 
					tupleStoreName.getDistributionGroup(), 
					dgroupDir);

			final boolean mkdirResult = dgroupDirHandle.mkdirs();

			assert (mkdirResult == true) : "Unable to create dir: " + dgroupDirHandle;

			try {
				writeDistributionGroupMetaData();
			} catch (Exception e) {
				logger.error("Unable to write meta data", e);
			}
		}

		final String ssTableDir = SSTableHelper.getSSTableDir(storageDir, tupleStoreName);
		final File ssTableDirHandle = new File(ssTableDir);

		if(! ssTableDirHandle.exists()) {
			logger.info("Create a new dir for table {} ({}) ", tupleStoreName.getFullname(), ssTableDirHandle);
			ssTableDirHandle.mkdir();
		}
	}

	/**
	 * Write the meta data for the distribution group
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 * @throws IOException 
	 */
	protected void writeDistributionGroupMetaData() throws ZookeeperException, 
	ZookeeperNotFoundException, IOException {

		if(! tupleStoreName.isDistributedTable()) {	
			return;
		}

		logger.debug("Write meta data for distribution group: ", tupleStoreName.getDistributionGroup());

		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		final DistributionGroupZookeeperAdapter dAdapter = new DistributionGroupZookeeperAdapter(zookeeperClient);
		final String version = dAdapter.getVersionForDistributionGroup(tupleStoreName.getDistributionGroup(), null);

		DistributionGroupMetadata distributionGroupMetadata = new DistributionGroupMetadata();
		distributionGroupMetadata.setVersion(version);

		DistributionGroupMetadataHelper.writeMedatadataForGroup(storage.getBasedir().getAbsolutePath(), 
				tupleStoreName.getDistributionGroupObject(), 
				distributionGroupMetadata);
	}

	/**
	 * Scan the database directory for all existing SSTables and
	 * create reader objects
	 * @throws StorageManagerException 
	 * 
	 */
	protected void scanForExistingTables() throws StorageManagerException {
		logger.info("Scan for existing SSTables: " + tupleStoreName.getFullname());
		final String storageDir = storage.getBasedir().getAbsolutePath();
		final String ssTableDir = SSTableHelper.getSSTableDir(storageDir, tupleStoreName);
		final File directoryHandle = new File(ssTableDir);

		checkSSTableDir(directoryHandle);

		final File[] entries = directoryHandle.listFiles();

		for(final File file : entries) {
			final String filename = file.getName();
			if(SSTableHelper.isFileNameSSTable(filename)) {
				logger.info("Found sstable: " + filename);

				try {
					final int sequenceNumber = SSTableHelper.extractSequenceFromFilename(tupleStoreName, filename);
					final SSTableFacade facade = new SSTableFacade(storageDir, tupleStoreName, sequenceNumber, 
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
	public static boolean deletePersistentTableData(final String dataDirectory, final TupleStoreName sstableName) {
		logger.info("Delete all existing SSTables for relation: {}", sstableName.getFullname());

		final File directoryHandle = new File(SSTableHelper.getSSTableDir(dataDirectory, sstableName));

		// Does the directory exist?
		if(! directoryHandle.isDirectory()) {
			return true;
		}

		final File[] entries = directoryHandle.listFiles();

		for(final File file : entries) {			
			deleteFileIfKnown(file);
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
	 * Delete the file if it belongs to BBoxDB
	 * @param file
	 */
	protected static void deleteFileIfKnown(final File file) {

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
		} else if(SSTableHelper.isFileNameMetadata(filename)) {
			logger.info("Deleting meta file: {}", file);
			file.delete();
		} else if(SSTableHelper.isFileNameSpatialIndex(filename)) {
			logger.info("Deleting spatial index file: {}", file);
			file.delete();
		} else {
			logger.warn("NOT deleting unknown file: {}", file);
		}
	}

	/**
	 * Search for the most recent version of the tuple
	 * @param key
	 * @return The tuple or null
	 * @throws StorageManagerException
	 */
	public List<Tuple> get(final String key) throws StorageManagerException {

		if(! serviceState.isInRunningState()) {
			throw new StorageManagerException("Storage manager is not ready: " 
					+ tupleStoreName.getFullname() + " state: " + serviceState);
		}

		final Summary.Timer requestTimer = getRequestLatency.startTimer();

		final List<ReadOnlyTupleStore> aquiredStorages = new ArrayList<>();
		final List<Tuple> tupleList = new ArrayList<>();

		try {
			aquiredStorages.addAll(aquireStorage());

			for(final ReadOnlyTupleStore tupleStorage : aquiredStorages) {
				final List<Tuple> resultTuples = tupleStorage.get(key);
				tupleList.addAll(resultTuples);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			releaseStorage(aquiredStorages);
			requestTimer.observeDuration();
		}

		final DuplicateResolver<Tuple> resolver = TupleDuplicateResolverFactory.build(tupleStoreConfiguration);
		resolver.removeDuplicates(tupleList);

		return tupleList;
	}	

	/**
	 * Try to acquire all needed tables
	 * @return 
	 * @throws StorageManagerException 
	 */
	public List<ReadOnlyTupleStore> aquireStorage() throws StorageManagerException {

		for(int execution = 0; execution < Const.OPERATION_RETRY; execution++) {

			final List<ReadOnlyTupleStore> aquiredStorages = new ArrayList<>();
			final List<ReadOnlyTupleStore> knownStorages = tupleStoreInstances.getAllTupleStorages();

			for(final ReadOnlyTupleStore tupleStorage : knownStorages) {
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
	public void releaseStorage(List<ReadOnlyTupleStore> storagesToRelease) {
		
		if(storagesToRelease == null) {
			return;
		}
		
		for(final ReadOnlyTupleStore storage : storagesToRelease) {
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
	public TupleStoreName getTupleStoreName() {
		return tupleStoreName;
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
		final Memtable memtable = new Memtable(tupleStoreName, 
				configuration.getMemtableEntriesMax(), 
				configuration.getMemtableSizeMax());

		memtable.acquire();
		memtable.init();

		final Memtable oldMemtable = tupleStoreInstances.activateNewMemtable(memtable);	

		if(oldMemtable != null) {
			final MemtableAndTupleStoreManagerPair memtableTask 
			= new MemtableAndTupleStoreManagerPair(oldMemtable, this);

			storage.scheduleMemtableFlush(memtableTask);
		}

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
					+ tupleStoreName.getFullname() 
					+ " state: " + serviceState);
		}

		if(tupleStoreInstances.getState() == TupleStoreManagerState.READ_ONLY) {
			throw new RejectedException("Storage manager is in read only state: " + tupleStoreName);
		}

		try {
			// Ensure that only one memtable is newly created
			synchronized (this) {	
				if(getMemtable().isFull()) {
					initNewMemtable();
				}

				getMemtable().put(tuple);
			}

			// Notify callbacks
			insertCallbacks.forEach(c -> c.accept(tuple));

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
					+ tupleStoreName.getFullname() 
					+ " state: " + serviceState);
		}

		if(tupleStoreInstances.getState() == TupleStoreManagerState.READ_ONLY) {
			throw new RejectedException("Storage manager is in read only state: " + tupleStoreName);
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
			throw new RejectedException("Storage manager is in read only state: " + tupleStoreName);
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
			throw new RejectedException("Storage manager is in read only state: " + tupleStoreName);
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
	public List<ReadOnlyTupleStore> getAllInMemoryStorages() {
		return tupleStoreInstances.getAllInMemoryStorages();
	}

	/**
	 * Get all tuple storages delegate
	 * @return
	 */
	public List<ReadOnlyTupleStore> getAllTupleStorages() {
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
		List<ReadOnlyTupleStore> storages = null;

		try {
			storages = aquireStorage();

			return storages.stream()
					.mapToLong(s -> s.getSize())
					.sum();

		} finally {
			if(storages != null) {
				releaseStorage(storages);
			}
		}
	}
	
	/**
	 * Get the amount of tuples
	 * @return
	 * @throws StorageManagerException
	 */
	public long getNumberOfTuples() throws StorageManagerException {
		List<ReadOnlyTupleStore> storages = null;

		try {
			storages = aquireStorage();

			return storages.stream()
					.mapToLong(s -> s.getNumberOfTuples())
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

	/**
	 * Get the tuple store configuration
	 * @return
	 */
	public TupleStoreConfiguration getTupleStoreConfiguration() {
		return tupleStoreConfiguration;
	}

	/**
	 * Register a new insert callback
	 * @param callback
	 */
	public void registerInsertCallback(final Consumer<Tuple> callback) {
		insertCallbacks.add(callback);
	}

	/**
	 * Remove a insert callback
	 * @return 
	 */
	public boolean removeInsertCallback(final Consumer<Tuple> callback) {
		return insertCallbacks.remove(callback);
	}
	
	/**
	 * Get the most recent version of the tuple
	 * e.g. Memtables can contain multiple versions of the key
	 * The iterator can return an outdated version
	 * 
	 * @param tuple
	 * @return
	 * @throws StorageManagerException 
	 */
	public List<Tuple> getVersionsForTuple(final String key) 
			throws StorageManagerException {
		
		final List<Tuple> resultTuples = getAllTupleVersionsForKey(key);
		
		final TupleStoreConfiguration tupleStoreConfiguration 
			= getTupleStoreConfiguration();
				
		final DuplicateResolver<Tuple> resolver 
			= TupleDuplicateResolverFactory.build(tupleStoreConfiguration);
				
		// Removed unwanted tuples for key
		resolver.removeDuplicates(resultTuples);
		
		return resultTuples;
	}

	/**
	 * Get all tuples for a given key
	 * @param tuple
	 * @param activeStorage
	 * @return
	 * @throws StorageManagerException
	 */
	public List<Tuple> getAllTupleVersionsForKey(final String key) throws StorageManagerException {
		
		List<ReadOnlyTupleStore> aquiredStorages = null;
		
		try {
			aquiredStorages = aquireStorage();
			
			final List<Tuple> resultTuples = new ArrayList<>();
			
			for(final ReadOnlyTupleStore readOnlyTupleStorage : aquiredStorages) {
				final List<Tuple> possibleTuples = readOnlyTupleStorage.get(key);
				resultTuples.addAll(possibleTuples);
			}
			
			return resultTuples;
		} catch(Exception e) {
			throw e;
		} finally {
			releaseStorage(aquiredStorages);
		}
	}
}
