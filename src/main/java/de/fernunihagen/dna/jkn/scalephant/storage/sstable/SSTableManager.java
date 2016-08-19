package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.jkn.scalephant.ScalephantService;
import de.fernunihagen.dna.jkn.scalephant.storage.Memtable;
import de.fernunihagen.dna.jkn.scalephant.storage.Storage;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.DeletedTuple;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.compact.SSTableCompactorThread;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.reader.SSTableFacade;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.reader.SSTableKeyIndexReader;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.reader.SSTableReader;
import de.fernunihagen.dna.jkn.scalephant.util.State;
import de.fernunihagen.dna.jkn.scalephant.util.Stoppable;

public class SSTableManager implements ScalephantService, Storage {
	
	/**
	 * The name of the table
	 */
	protected final SSTableName name;
	
	/**
	 * The Storage configuration
	 */
	protected final ScalephantConfiguration storageConfiguration;
	
	/**
	 * The number of the table
	 */
	protected AtomicInteger tableNumber;
	
	/**
	 * The reader for existing SSTables
	 */
	protected final List<SSTableFacade> sstableFacades;

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

	public SSTableManager(final State storageState, final SSTableName name, final ScalephantConfiguration storageConfiguration) {
		super();

		this.storageConfiguration = storageConfiguration;
		this.storageState = storageState;
		this.name = name;
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
		
		logger.info("Init a new instance for the table: " + getName());
		
		unflushedMemtables.clear();
		sstableFacades.clear();
		runningThreads.clear();
		createSSTableDirIfNeeded();
		
		try {
			scanForExistingTables();
		} catch (StorageManagerException e) {
			logger.error("Unable to init the instance: " + getName(), e);
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
	 * Start the checkpoint thread if needed
	 */
	protected void startCheckpointThread() {
		if(storageConfiguration.getStorageCheckpointInterval() > 0) {
			final int maxUncheckpointedSeconds = storageConfiguration.getStorageCheckpointInterval();
			final SSTableCheckpointThread ssTableCheckpointThread = new SSTableCheckpointThread(maxUncheckpointedSeconds);
			final Thread checkpointThread = new Thread(ssTableCheckpointThread);
			checkpointThread.start();
			runningThreads.add(checkpointThread);
		} else {
			logger.info("NOT starting the checkpoint thread for: " + getName());
		}
	}
	
	/**
	 * Start the compact thread if needed
	 */
	protected void startCompactThread() {
		if(storageConfiguration.isStorageRunCompactThread()) {
			final Thread compactThread = new Thread(new SSTableCompactorThread(this));
			compactThread.setName("Compact thread for: " + getName());
			compactThread.start();
			runningThreads.add(compactThread);
		} else {
			logger.info("NOT starting the sstable compact thread for: " + getName());
		}
	}

	/**
	 * Start the memtable flush thread if needed
	 */
	protected void startMemtableFlushThread() {
		if(storageConfiguration.isStorageRunMemtableFlushThread()) {
			final SSTableFlushThread ssTableFlushThread = new SSTableFlushThread(this);
			final Thread flushThread = new Thread(ssTableFlushThread);
			flushThread.setName("Memtable flush thread for: " + getName());
			flushThread.start();
			runningThreads.add(flushThread);
			stoppableTasks.add(ssTableFlushThread);
		} else {
			logger.info("NOT starting the memtable flush thread for:" + getName());
		}
	}

	/**
	 * Shutdown the instance
	 */
	@Override
	public void shutdown() {
		logger.info("Shuting down the instance for table: " + getName());
		
		// Set ready to false. The threads will shutdown after completing
		// the running tasks
		ready = false;
		
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
		final File rootDir = new File(storageConfiguration.getDataDirectory());		
		final File directoryHandle = new File(SSTableHelper.getSSTableDir(storageConfiguration.getDataDirectory(), name.getFullname()));
		
		if(rootDir.exists() && ! directoryHandle.exists()) {
			logger.info("Create a new dir for table: " + getName());
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
		logger.info("Scan for existing SSTables: " + getName());
		final File directoryHandle = new File(SSTableHelper.getSSTableDir(storageConfiguration.getDataDirectory(), name.getFullname()));
		
	    checkSSTableDir(directoryHandle);
	
		final File[] entries = directoryHandle.listFiles();
				
		for(final File file : entries) {
			final String filename = file.getName();
			if(isFileNameSSTable(filename)) {
				logger.info("Found sstable: " + filename);
				
				try {
					final int sequenceNumber = SSTableHelper.extractSequenceFromFilename(name, filename);
					final SSTableFacade facade = new SSTableFacade(storageConfiguration.getDataDirectory(), name, sequenceNumber);
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
	 * Delete all existing SSTables in the given directory
	 * 
	 * @return Directory was deleted or not
	 * @throws StorageManagerException 
	 */
	public boolean deleteExistingTables() throws StorageManagerException {
		logger.info("Delete all existing SSTables for relation: " + getName());
		final File directoryHandle = new File(SSTableHelper.getSSTableDir(storageConfiguration.getDataDirectory(), name.getFullname()));
	
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
	public void flushMemtable(final Memtable memtable) throws StorageManagerException {
		
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
			
		// Read unflushed memtables first
		Tuple tuple = getTupleFromMemtable(key);
				
		boolean readComplete = false;
		while(! readComplete) {
			readComplete = true;
		
			// Read data from the persistent SSTables
			for(final SSTableFacade facade : sstableFacades) {
				boolean canBeUsed = facade.acquire();
				
				if(! canBeUsed ) {
					readComplete = false;
					break;
				}
				
				final SSTableKeyIndexReader indexReader = facade.getSsTableKeyIndexReader();
				final SSTableReader reader = facade.getSsTableReader();
				
				final int position = indexReader.getPositionForTuple(key);
				
				// Found a tuple
				if(position != -1) {
					final Tuple tableTuple = reader.getTupleAtPosition(position);
					if(tuple == null) {
						tuple = tableTuple;
					} else if(tableTuple.getTimestamp() > tuple.getTimestamp()) {
						tuple = tableTuple;
					}
				}
				
				facade.release();
			}
		}
		
		if(tuple instanceof DeletedTuple) {
			return null;
		}
		
		return tuple;
	}
	
	/**
	 * Get all tuples that are inside of the bounding box
	 * @param boundingBox
	 * @return
	 * @throws StorageManagerException 
	 */
	public Collection<Tuple> getTuplesInside(final BoundingBox boundingBox) throws StorageManagerException {
	
		final List<Tuple> resultList = new ArrayList<Tuple>();
		
		// Read unflushed memtables first
		for(final Memtable unflushedMemtable : unflushedMemtables) {
			try {
				final Collection<Tuple> memtableResult = unflushedMemtable.getTuplesInside(boundingBox);
				resultList.addAll(memtableResult);
			} catch (StorageManagerException e) {
				logger.warn("Got an exception while scanning unflushed memtable: ", e);
			}
		}
		
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
				
				final SSTableKeyIndexReader indexReader = facade.getSsTableKeyIndexReader();
								
				for (final Tuple tuple : indexReader) {
					if(tuple.getBoundingBox().overlaps(boundingBox)) {
						storedTuples.add(tuple);
					}
				}
				
				facade.release();
			}
		}
		resultList.addAll(storedTuples);
		return resultList;
	}

	@Override
	public Collection<Tuple> getTuplesAfterTime(final long timestamp)
			throws StorageManagerException {
		
		final List<Tuple> resultList = new ArrayList<Tuple>();

		// Read unflushed memtables first
		for(final Memtable unflushedMemtable : unflushedMemtables) {
			try {
				final Collection<Tuple> memtableResult = unflushedMemtable.getTuplesAfterTime(timestamp);
				resultList.addAll(memtableResult);
			} catch (StorageManagerException e) {
				logger.warn("Got an exception while scanning unflushed memtable: ", e);
			}
		}
		
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
		
		resultList.addAll(storedTuples);
		return resultList;
	}
	
	/**
	 * Get the tuple from the unflushed memtables
	 * @param key
	 * @return
	 */
	protected Tuple getTupleFromMemtable(final String key) {
		
		Tuple result = null;
		
		for(final Memtable unflushedMemtable : unflushedMemtables) {
			final Tuple tuple = unflushedMemtable.get(key);
			
			if(tuple != null) {
				if(result == null) {
					result = tuple;
					continue;
				}
				
				// Get the most recent version of the tuple
				if(tuple.compareTo(result) < 0) {
					result = tuple;
					continue;	
				}
			}
		}
		
		return result;
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
	public SSTableName getName() {
		return name;
	}

	/**
	 * Returns the configuration
	 * @return
	 */
	public ScalephantConfiguration getStorageConfiguration() {
		return storageConfiguration;
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

	// These methods are required by the interface
	@Override
	public void put(final Tuple tuple) throws StorageManagerException {
		throw new UnsupportedOperationException("SSTables are read only");
	}

	@Override
	public void delete(final String key) throws StorageManagerException {
		throw new UnsupportedOperationException("SSTables are read only");
	}

	@Override
	public void clear() throws StorageManagerException {
		throw new UnsupportedOperationException("SSTables are read only");
	}
}
