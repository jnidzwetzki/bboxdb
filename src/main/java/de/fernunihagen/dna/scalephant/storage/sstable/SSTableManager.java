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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.scalephant.ScalephantService;
import de.fernunihagen.dna.scalephant.storage.Memtable;
import de.fernunihagen.dna.scalephant.storage.Storage;
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.DeletedTuple;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.compact.SSTableCompactorThread;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableFacade;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableKeyIndexReader;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.TupleByKeyLocator;
import de.fernunihagen.dna.scalephant.util.State;
import de.fernunihagen.dna.scalephant.util.Stoppable;

public class SSTableManager implements ScalephantService, Storage {
	
	/**
	 * The name of the table
	 */
	protected final SSTableName sstablename;
	
	/**
	 * The Storage configuration
	 */
	protected final ScalephantConfiguration scalephantConfiguration;
	
	/**
	 * The number of the table
	 */
	protected AtomicInteger tableNumber;
	
	/**
	 * The reader for existing SSTables
	 */
	protected final List<SSTableFacade> sstableFacades;

	/**
	 * The active memtable
	 */
	protected volatile Memtable memtable;
	
	/**
	 * The unflushed memtables
	 */
	protected List<Memtable> unflushedMemtables;

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
	protected final Set<Thread> runningThreads;
	
	/**
	 * The stoppable tasks
	 */
	protected final Set<Stoppable> stoppableTasks;
	
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
		
		this.unflushedMemtables = new CopyOnWriteArrayList<Memtable>();
		this.sstableFacades = new CopyOnWriteArrayList<SSTableFacade>();
		this.runningThreads = new HashSet<Thread>();
		this.stoppableTasks = new HashSet<Stoppable>();
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
		
		unflushedMemtables.clear();
		sstableFacades.clear();
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
		memtable = new Memtable(sstablename, 
				scalephantConfiguration.getMemtableEntriesMax(), 
				scalephantConfiguration.getMemtableSizeMax());
		
		memtable.init();
	}

	/**
	 * Start the checkpoint thread if needed
	 */
	protected void startCheckpointThread() {
		if(scalephantConfiguration.getStorageCheckpointInterval() > 0) {
			final int maxUncheckpointedSeconds = scalephantConfiguration.getStorageCheckpointInterval();
			final SSTableCheckpointThread ssTableCheckpointThread = new SSTableCheckpointThread(maxUncheckpointedSeconds, this);
			final Thread checkpointThread = new Thread(ssTableCheckpointThread);
			checkpointThread.setName("Checkpoint thread for: " + getSSTableName());
			checkpointThread.start();
			runningThreads.add(checkpointThread);
			stoppableTasks.add(ssTableCheckpointThread);
		} else {
			logger.info("NOT starting the checkpoint thread for: " + getSSTableName());
		}
	}
	
	/**
	 * Start the compact thread if needed
	 */
	protected void startCompactThread() {
		if(scalephantConfiguration.isStorageRunCompactThread()) {
			final Thread compactThread = new Thread(new SSTableCompactorThread(this));
			compactThread.setName("Compact thread for: " + getSSTableName());
			compactThread.start();
			runningThreads.add(compactThread);
		} else {
			logger.info("NOT starting the sstable compact thread for: " + getSSTableName());
		}
	}

	/**
	 * Start the memtable flush thread if needed
	 */
	protected void startMemtableFlushThread() {
		if(scalephantConfiguration.isStorageRunMemtableFlushThread()) {
			final SSTableFlushThread ssTableFlushThread = new SSTableFlushThread(this);
			final Thread flushThread = new Thread(ssTableFlushThread);
			flushThread.setName("Memtable flush thread for: " + getSSTableName());
			flushThread.start();
			runningThreads.add(flushThread);
			stoppableTasks.add(ssTableFlushThread);
		} else {
			logger.info("NOT starting the memtable flush thread for:" + getSSTableName());
		}
	}

	/**
	 * Shutdown the instance
	 */
	@Override
	public void shutdown() {
		logger.info("Shuting down the instance for table: " + getSSTableName());
		
		// Set ready to false. The threads will shutdown after completing
		// the running tasks
		ready = false;
		storageState.setReady(false);
		
		memtable.shutdown();
		
		// Flush in memory data
		try {
			flushMemtable(memtable);
		} catch (StorageManagerException e) {
			logger.warn("Got exception while flushing pending memtable to disk", e);
		}
		
		stopThreads();

		// Close all sstables
		for(final SSTableFacade facade : sstableFacades) {
			facade.shutdown();
		}
		sstableFacades.clear();
	}

	/**
	 * Shutdown all running service threads
	 */
	public void stopThreads() {
		
		// Stop the running tasks
		for(final Stoppable stoppable : stoppableTasks) {
			stoppable.stop();
		}
		
		// Stop the corresponsing threads
		for(final Thread thread : runningThreads) {
			logger.info("Interrupt and join thread: " + thread.getName());
			thread.interrupt();
			
			try {
				thread.join(THREAD_WAIT_TIMEOUT);
			} catch (InterruptedException e) {
				logger.warn("Got exception while waiting on thread join: " + thread.getName(), e);
			}
		}
	}
	
	
	/**
	 * Is the shutdown complete?
	 * 
	 * @return
	 */
	public boolean isShutdownComplete() {
		
		for(final Thread thread : runningThreads) {
			if(thread.isAlive()) {
				return false;
			}
		}

		return true;
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
					sstableFacades.add(facade);
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
		
		for(final SSTableFacade facade : sstableFacades) {
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
		logger.info("Delete all existing SSTables for relation: " + getSSTableName());
		
		// Reject new writes
		shutdown();
		
		// Wait for in memory data flush
		while(! isShutdownComplete()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				return false;
			}
		}
		
		if(! memtable.isEmpty()) {
			logger.warn("Memtable is not empty after shutdown()");
			memtable.clear();
		}
		
		if(! unflushedMemtables.isEmpty()) {
			logger.warn("There are unflsuhed memtables after shutdown(): " + unflushedMemtables);
			unflushedMemtables.clear();
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
				logger.info("Deleting file: " + file);
				file.delete();
			} else if(isFileNameSSTableIndex(filename)) {
				logger.info("Deleting index file: " + file);
				file.delete();
			} else if(isFileNameSSTableMetadata(filename)) {
				logger.info("Deleting meta file: " + file);
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
	 * Belongs the given filename to a SSTable meta file?
	 * @param filename
	 * @return
	 */
	protected boolean isFileNameSSTableMetadata(final String filename) {
		return filename.startsWith(SSTableConst.SST_FILE_PREFIX) 
				&& filename.endsWith(SSTableConst.SST_META_SUFFIX);
	}
	
	/**
	 * Schedule a memtable for flush
	 * 
	 * @param memtable
	 * @throws StorageManagerException
	 */
	protected void flushMemtable(final Memtable memtable) throws StorageManagerException {
		
		// Empty memtables don't need to be flushed to disk
		if(memtable.isEmpty()) {
			return;
		}
		
		synchronized (unflushedMemtables) {
			unflushedMemtables.add(memtable);
			unflushedMemtables.notifyAll();
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
			throw new StorageManagerException("Storage manager is not ready");
		}
		
		// Is tuple stored in the current memtable?
		final Tuple memtableTuple = memtable.get(key);

		// If this is true, this is the most recent version
		if(memtableTuple != null) {
			return SSTableHelper.replaceDeletedTupleWithNull(memtableTuple);
		}
		
		// Otherwise scan unflushed memtables and SStables
		final TupleByKeyLocator tupleByKeyLocator = new TupleByKeyLocator(this);
		return tupleByKeyLocator.getMostRecentTuple(key);
	}
	
	
	/**
	 * Get the a collection with the most recent version of the tuples
	 * DeletedTuples are removed from the result set
	 * 
	 * @param tupleList
	 * @return
	 */
	protected Collection<Tuple> getTheMostRecentTuples(final Collection<Tuple> tupleList) {
		
		final HashMap<String, Tuple> allTuples = new HashMap<String, Tuple>();

		// Find the most recent version of the tuple
		for(final Tuple tuple : tupleList) {
			
			final String tupleKey = tuple.getKey();
			
			if(! allTuples.containsKey(tupleKey)) {
				allTuples.put(tupleKey, tuple);
			} else {
				final long knownTimestamp = allTuples.get(tupleKey).getTimestamp();
				final long newTimestamp = tuple.getTimestamp();
				
				// Update with an newer version
				if(newTimestamp > knownTimestamp) {
					allTuples.put(tupleKey, tuple);
				}
			}
		}
		
		// Remove deleted tuples from result
		for(final Tuple tuple : allTuples.values()) {
			if(tuple instanceof DeletedTuple) {
				allTuples.remove(tuple.getKey());
			}
		}
		
		return allTuples.values();
	}
	
	/**
	 * Get all tuples that are inside of the bounding box
	 * @param boundingBox
	 * @return
	 * @throws StorageManagerException 
	 */
	public Collection<Tuple> getTuplesInside(final BoundingBox boundingBox) throws StorageManagerException {
	
		final List<Tuple> resultList = new ArrayList<Tuple>();
		
		// Query memtable
		final Collection<Tuple> memtableTuples = memtable.getTuplesInside(boundingBox);
		resultList.addAll(memtableTuples);
		
		// Query unflushed memtables
		for(final Memtable unflushedMemtable : unflushedMemtables) {
			try {
				final Collection<Tuple> memtableResult = unflushedMemtable.getTuplesInside(boundingBox);
				resultList.addAll(memtableResult);
			} catch (StorageManagerException e) {
				logger.warn("Got an exception while scanning unflushed memtable: ", e);
			}
		}
		
		// Query the sstables
		final List<Tuple> storedTuples = getTuplesInsideFromSStable(boundingBox);
		resultList.addAll(storedTuples);
		
		return getTheMostRecentTuples(resultList);
	}

	/**
	 * Get all tuples that are inside the given bounding box from the sstables
	 * @param timestamp
	 * @return
	 */
	protected List<Tuple> getTuplesInsideFromSStable(final BoundingBox boundingBox) {
		
		boolean readComplete = false;
		final List<Tuple> storedTuples = new ArrayList<Tuple>();

		while(! readComplete) {
			readComplete = true;
			storedTuples.clear();
			
			// Read data from the persistent SSTables
			for(final SSTableFacade facade : sstableFacades) {
				boolean canBeUsed = facade.acquire();
				
				if(! canBeUsed ) {
					readComplete = false;
					break;
				}
				
				final SSTableKeyIndexReader indexReader = facade.getSsTableKeyIndexReader();
								
				for (final Tuple tuple : indexReader) {
					if(tuple.getBoundingBox().overlaps(boundingBox)) {
						storedTuples.add(tuple);
					}
				}
				
				facade.release();
			}
		}
		return storedTuples;
	}

	@Override
	public Collection<Tuple> getTuplesAfterTime(final long timestamp)
			throws StorageManagerException {
	
		final List<Tuple> resultList = new ArrayList<Tuple>();
		
		// Query active memtable
		final Collection<Tuple> memtableTuples = memtable.getTuplesAfterTime(timestamp);
		resultList.addAll(memtableTuples);

		// Query unflushed memtables
		for(final Memtable unflushedMemtable : unflushedMemtables) {
			try {
				final Collection<Tuple> memtableResult = unflushedMemtable.getTuplesAfterTime(timestamp);
				resultList.addAll(memtableResult);
			} catch (StorageManagerException e) {
				logger.warn("Got an exception while scanning unflushed memtable: ", e);
			}
		}
		
		// Query sstables
		final List<Tuple> storedTuples = getTuplesAfterTimeFromSSTable(timestamp);
		resultList.addAll(storedTuples);
		
		return getTheMostRecentTuples(resultList);
	}

	/**
	 * Get all tuples that are newer than the given timestamp from the sstables
	 * @param timestamp
	 * @return
	 */
	protected List<Tuple> getTuplesAfterTimeFromSSTable(final long timestamp) {
		
		// Scan the sstables
		boolean readComplete = false;
		final List<Tuple> storedTuples = new ArrayList<Tuple>();

		while(! readComplete) {
			readComplete = true;
			storedTuples.clear();
			
			// Read data from the persistent SSTables
			for(final SSTableFacade facade : sstableFacades) {
				boolean canBeUsed = facade.acquire();
				
				if(! canBeUsed ) {
					readComplete = false;
					break;
				}
				
				// Scan only tables that contain newer tuples
				if(facade.getSsTableMetadata().getNewestTuple() > timestamp) {
					final SSTableKeyIndexReader indexReader = facade.getSsTableKeyIndexReader();
					for (final Tuple tuple : indexReader) {
						if(tuple.getTimestamp() > timestamp) {
							storedTuples.add(tuple);
						}
					}
				}
				
				facade.release();
			}
		}
		return storedTuples;
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
	 * Get the sstable facades
	 * @return
	 */
	public List<SSTableFacade> getSstableFacades() {
		return sstableFacades;
	}
	
	/**
	 * Get the unflushed memtables
	 * @return
	 */
	public List<Memtable> getUnflushedMemtables() {
		return unflushedMemtables;
	}
	
	/**
	 * Flush the open memtable to disk
	 * @throws StorageManagerException
	 */
	public void flushMemtable() throws StorageManagerException {
		flushMemtable(memtable);
		initNewMemtable();
	}

	// These methods are required by the interface
	@Override
	public void put(final Tuple tuple) throws StorageManagerException {
		if(! storageState.isReady()) {
			throw new StorageManagerException("Storage manager is not ready");
		}
		
		// Ensure that only one memtable is newly created
		synchronized (this) {	
			if(memtable.isFull()) {
				flushMemtable();
			}
			
			memtable.put(tuple);
		}
	}

	@Override
	public void delete(final String key) throws StorageManagerException {
		if(! storageState.isReady()) {
			throw new StorageManagerException("Storage manager is not ready");
		}
		
		// Ensure that only one memtable is newly created
		synchronized (this) {	
			if(memtable.isFull()) {
				flushMemtable();
			}
			
			memtable.delete(key);
		}
	}

	@Override
	public void clear() throws StorageManagerException {
		deleteExistingTables();
		init();
	}
	
	/**
	 * Get the active memtable
	 * @return
	 */
	public Memtable getMemtable() {
		return memtable;
	}
}
