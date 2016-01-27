package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;
import de.fernunihagen.dna.jkn.scalephant.storage.DeletedTuple;
import de.fernunihagen.dna.jkn.scalephant.storage.Memtable;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;
import de.fernunihagen.dna.jkn.scalephant.util.State;

public class SSTableManager implements Lifecycle {
	
	/**
	 * The name of the table
	 */
	private final String name;
	
	/**
	 * The directory for the SSTables
	 */
	private final String directory;
	
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
	protected final Map<SSTableReader, SSTableIndexReader> indexReader;
	
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

	public SSTableManager(final State storageState, final String name, final String directory) {
		super();
		
		this.storageState = storageState;
		this.name = name;
		this.directory = directory;
		this.tableNumber = new AtomicInteger();
		this.tableNumber.set(0);
		
		unflushedMemtables = new CopyOnWriteArrayList<Memtable>();
		sstableReader = new CopyOnWriteArrayList<SSTableReader>();
		indexReader = Collections.synchronizedMap(new HashMap<SSTableReader, SSTableIndexReader>());
	}

	/**
	 * Init the instance
	 * 
	 */
	@Override
	public void init() {
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
		
		ready = true;
		flushThread = new Thread(new SSTableFlushThread(this));
		flushThread.setName("Memtable flush thread for: " + getName());
		flushThread.start();
		
		compactThread = new Thread(new SSTableCompactorThread(this));
		compactThread.setName("Compact thread for: " + getName());
		compactThread.start();
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
		
		for(final SSTableIndexReader reader : indexReader.values()) {
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
		final File rootDir = new File(getDirectory());		
		final File directoryHandle = new File(getSSTableDir(getDirectory(), getName()));
		
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
		final File directoryHandle = new File(getSSTableDir(getDirectory(), getName()));
		
	    checkSSTableDir(directoryHandle);
	
		final File[] entries = directoryHandle.listFiles();
				
		for(final File file : entries) {
			final String filename = file.getName();
			if(isFileNameSSTable(filename)) {
				logger.info("Found sstable: " + filename);
				
				try {
					final SSTableReader reader = new SSTableReader(getName(), getDirectory(), file);
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
		final File directoryHandle = new File(getSSTableDir(getDirectory(), getName()));
	
		checkSSTableDir(directoryHandle);

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
	protected SSTableIndexReader getIndexReaderForTable(final SSTableReader reader) throws StorageManagerException {
		
		if(! indexReader.containsKey(reader)) {
			if(! reader.isReady()) {
				reader.init();
			}
			
			final SSTableIndexReader tableIndexReader = new SSTableIndexReader(reader);
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
			
		// Read memtables first
		Tuple tuple = getTupleFromMemtable(key);
				
		boolean readComplete = false;
		while(! readComplete) {
			readComplete = true;
		
			// Read data from the persistent SSTables
			for(final SSTableReader reader : sstableReader) {
				final SSTableIndexReader indexReader = getIndexReaderForTable(reader);
				boolean canBeUsed = indexReader.acquire();
				
				if(! canBeUsed ) {
					logger.info("Got unusable sstable: " + indexReader);
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
	 * Get the tuple from the unflushed memtables
	 * @param key
	 * @return
	 */
	protected Tuple getTupleFromMemtable(final String key) {
		
		// Read tuple from unflushed memtables
		for(final Memtable unflushedMemtable : unflushedMemtables) {
			final Tuple tuple = unflushedMemtable.get(key);

			if(tuple != null) {
				return tuple;
			}
		}
		
		return null;
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

	public String getDirectory() {
		return directory;
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
	public Map<SSTableReader, SSTableIndexReader> getIndexReader() {
		return indexReader;
	}

}
