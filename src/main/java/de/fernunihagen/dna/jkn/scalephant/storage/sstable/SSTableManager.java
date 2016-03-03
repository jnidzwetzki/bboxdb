package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;
import de.fernunihagen.dna.jkn.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.jkn.scalephant.storage.Memtable;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.DeletedTuple;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.jkn.scalephant.util.State;

public class SSTableManager implements Lifecycle {
	
	/**
	 * The name of the table
	 */
	private final String name;
	
	/**
	 * The Storage configuration
	 */
	private final ScalephantConfiguration storageConfiguration;
	
	/**
	 * The number of the table
	 */
	private AtomicInteger tableNumber;
	
	/**
	 * The reader for existing SSTables
	 */
	protected final List<SSTableReader> sstableReader;

	/**
	 * The index reader for the SSTables
	 */
	protected final Map<SSTableReader, SSTableKeyIndexReader> indexReader;
	
	/**
	 * The unflushed memtables
	 */
	protected List<Memtable> unflushedMemtables;

	/**
	 * Ready flag for flush thread
	 */
	protected volatile boolean ready;
	
	/**
	 * The Flush thread
	 */
	protected Thread flushThread;
	
	/**
	 * The compactification thread
	 */
	protected Thread compactThread;
	
	/**
	 * The corresponding storage manager state
	 */
	protected State storageState;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableManager.class);

	public SSTableManager(final State storageState, final String name, final ScalephantConfiguration storageConfiguration) {
		super();

		this.storageConfiguration = storageConfiguration;
		this.storageState = storageState;
		this.name = name;
		this.tableNumber = new AtomicInteger();
		this.tableNumber.set(0);
		ready = false;
		
		unflushedMemtables = new CopyOnWriteArrayList<Memtable>();
		sstableReader = new CopyOnWriteArrayList<SSTableReader>();
		indexReader = Collections.synchronizedMap(new HashMap<SSTableReader, SSTableKeyIndexReader>());
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
		indexReader.clear();
		sstableReader.clear();
		createSSTableDirIfNeeded();
		
		try {
			scanForExistingTables();
		} catch (StorageManagerException e) {
			// Unable to init the instance
			return;
		}
		
		this.tableNumber = new AtomicInteger();
		this.tableNumber.set(getLastSequencenumberFromReader());

		// Set to ready before the threads are started
		ready = true;

		if(storageConfiguration.isStorageRunMemtableFlushThread()) {
			flushThread = new Thread(new SSTableFlushThread(this));
			flushThread.setName("Memtable flush thread for: " + getName());
			flushThread.start();
		} else {
			logger.info("NOT starting the memtable flush thread.");
		}
		
		if(storageConfiguration.isStorageRunCompactThread()) {
			compactThread = new Thread(new SSTableCompactorThread(this));
			compactThread.setName("Compact thread for: " + getName());
			compactThread.start();
		} else {
			logger.info("NOT starting the sstable compact thread.");
		}
	}

	/**
	 * Shutdown the instance
	 * 
	 */
	@Override
	public void shutdown() {
		logger.info("Shuting down the instance for table: " + getName());
		ready = false;
		
		if(flushThread != null) {
			flushThread.interrupt();
		}
		
		if(compactThread != null) {
			compactThread.interrupt();
		}
		
		for(final SSTableKeyIndexReader reader : indexReader.values()) {
			reader.shutdown();
		}
		indexReader.clear();
		
		for(final AbstractTableReader reader : sstableReader) {
			reader.shutdown();
		}
		sstableReader.clear();
	}
	
	
	/**
	 * Is the shutdown complete?
	 * 
	 * @return
	 */
	public boolean isShutdownComplete() {
		boolean shutdownComplete = true;
		
		if(flushThread != null) {
			shutdownComplete = shutdownComplete & ! flushThread.isAlive();
		}
		
		if(compactThread != null) {
			shutdownComplete = shutdownComplete & ! compactThread.isAlive();
		}
		
		return shutdownComplete;
	}
	
	/**
	 * Ensure that the directory for the given table exists
	 * 
	 */
	protected void createSSTableDirIfNeeded() {
		final File rootDir = new File(storageConfiguration.getDataDirectory());		
		final File directoryHandle = new File(getSSTableDir(storageConfiguration.getDataDirectory(), getName()));
		
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
		final File directoryHandle = new File(getSSTableDir(storageConfiguration.getDataDirectory(), getName()));
		
	    checkSSTableDir(directoryHandle);
	
		final File[] entries = directoryHandle.listFiles();
				
		for(final File file : entries) {
			final String filename = file.getName();
			if(isFileNameSSTable(filename)) {
				logger.info("Found sstable: " + filename);
				
				try {
					final SSTableReader reader = new SSTableReader(getName(), storageConfiguration.getDataDirectory(), file);
					sstableReader.add(reader);
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
		
		for(AbstractTableReader reader : sstableReader) {
			final int sequenceNumber = reader.getTablebumber();
			
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
		final File directoryHandle = new File(getSSTableDir(storageConfiguration.getDataDirectory(), getName()));
	
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
	 * Schedule a memtable for flush
	 * 
	 * @param memtable
	 * @throws StorageManagerException
	 */
	public void flushMemtable(final Memtable memtable) throws StorageManagerException {
		synchronized (unflushedMemtables) {
			unflushedMemtables.add(memtable);
			unflushedMemtables.notifyAll();
		}
	}
	
	/**
	 * Get the index reader for a table reader
	 * 
	 * @param reader
	 * @return The index reader
	 * @throws StorageManagerException
	 */
	protected SSTableKeyIndexReader getIndexReaderForTable(final SSTableReader reader) throws StorageManagerException {
		
		if(! indexReader.containsKey(reader)) {
			if(! reader.isReady()) {
				reader.init();
			}
			
			final SSTableKeyIndexReader tableIndexReader = new SSTableKeyIndexReader(reader);
			tableIndexReader.init();
			indexReader.put(reader, tableIndexReader);
		}
		
		return indexReader.get(reader);
	}
	
	/**
	 * Search for the most recent version of the tuple
	 * @param key
	 * @return The tuple or null
	 * @throws StorageManagerException
	 */
	public Tuple get(final String key) throws StorageManagerException {
			
		// Read unlushed memtables first
		Tuple tuple = getTupleFromMemtable(key);
				
		boolean readComplete = false;
		while(! readComplete) {
			readComplete = true;
		
			// Read data from the persistent SSTables
			for(final SSTableReader reader : sstableReader) {
				final SSTableKeyIndexReader indexReader = getIndexReaderForTable(reader);
				boolean canBeUsed = indexReader.acquire();
				
				if(! canBeUsed ) {
					readComplete = false;
					break;
				}
				
				int position = indexReader.getPositionForTuple(key);
				
				// Found a tuple
				if(position != -1) {
					final Tuple tableTuple = reader.getTupleAtPosition(position);
					if(tuple == null) {
						tuple = tableTuple;
					} else if(tableTuple.getTimestamp() > tuple.getTimestamp()) {
						tuple = tableTuple;
					}
				}
				
				indexReader.release();
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
		while(! readComplete) {
			readComplete = true;
		
			// Read data from the persistent SSTables
			for(final SSTableReader reader : sstableReader) {
				final SSTableKeyIndexReader indexReader = getIndexReaderForTable(reader);
				boolean canBeUsed = indexReader.acquire();
				
				if(! canBeUsed ) {
					readComplete = false;
					break;
				}
								
				for (final Tuple tuple : indexReader) {
					if(tuple.getBoundingBox().overlaps(boundingBox)) {
						resultList.add(tuple);
					}
				}
				
				reader.release();
			}
		}
		
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
	 * The full name of the SSTable directoy for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/scalephant/data/relation1 
	 */
	protected static String getSSTableDir(final String directory, final String name) {
		return directory 
				+ File.separator 
				+ name;
	}
	
	/**
	 * The base name of the SSTable file for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/scalephant/data/relation1/sstable_relation1_2
	 */
	protected static String getSSTableBase(final String directory, final String name, int tablebumber) {
		return getSSTableDir(directory, name)
				+ File.separator 
				+ SSTableConst.SST_FILE_PREFIX 
				+ name 
				+ "_" 
				+ tablebumber;
	}
	
	/**
	 * The full name of the SSTable file for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/scalephant/data/relation1/sstable_relation1_2.sst
	 */
	protected static String getSSTableFilename(final String directory, final String name, int tablebumber) {
		return getSSTableBase(directory, name, tablebumber)
				+ SSTableConst.SST_FILE_SUFFIX;
	}
	
	/**
	 * The full name of the SSTable index file for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/scalephant/data/relation1/sstable_relation1_2.idx
	 */
	protected static String getSSTableIndexFilename(final String directory, final String name, int tablebumber) {
		return getSSTableBase(directory, name, tablebumber)
				+ SSTableConst.SST_INDEX_SUFFIX;
	}

	
	public int increaseTableNumber() {
		return tableNumber.getAndIncrement();
	}

	public boolean isReady() {
		return ready;
	}

	public void setReady(boolean ready) {
		this.ready = ready;
	}

	public String getName() {
		return name;
	}
	
	/**
	 * Get the sstable reader
	 * @return
	 */
	public List<SSTableReader> getSstableReader() {
		return sstableReader;
	}

	/**
	 * Get the index reader
	 * @return
	 */
	public Map<SSTableReader, SSTableKeyIndexReader> getIndexReader() {
		return indexReader;
	}

	/**
	 * Returns the configuration
	 * @return
	 */
	public ScalephantConfiguration getStorageConfiguration() {
		return storageConfiguration;
	}
	
}
