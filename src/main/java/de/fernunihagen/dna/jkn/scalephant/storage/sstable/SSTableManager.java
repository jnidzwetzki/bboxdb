package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

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
	protected final String name;
	
	/**
	 * The Directoy for the SSTables
	 */
	protected final String directory;
	
	/**
	 * The number of the table
	 */
	protected int tableNumber;
	
	/**
	 * The Reader for existing SSTables
	 */
	protected final List<SSTableReader> tableReader;

	/**
	 * The Index Reader for the SSTables
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
	 * The coresponding storage manager state
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
		this.tableNumber = 0;
		
		unflushedMemtables = new CopyOnWriteArrayList<Memtable>();
		tableReader = new CopyOnWriteArrayList<SSTableReader>();
		indexReader = Collections.synchronizedMap(new HashMap<SSTableReader, SSTableIndexReader>());
	}

	@Override
	public void init() {
		logger.info("Init a new instance for the table: " + name);
		unflushedMemtables.clear();
		indexReader.clear();
		tableReader.clear();
		createSSTableDirIfNeeded();
		scanForExistingTables();
		
		tableNumber = getLastSequencenumberFromReader();
		ready = true;
		flushThread = new Thread(new SSTableFlusher());
		flushThread.start();
		flushThread.setName("Memtable flush thread: " + name);
	}

	@Override
	public void shutdown() {
		logger.info("Shuting down the instance for table: " + name);
		ready = false;
		flushThread.interrupt();
		
		for(final SSTableIndexReader reader : indexReader.values()) {
			reader.shutdown();
		}
		indexReader.clear();
		
		for(final AbstractTableReader reader : tableReader) {
			reader.shutdown();
		}
		tableReader.clear();
	}
	
	
	/**
	 * Is the shutdown complete?
	 * 
	 * @return
	 */
	public boolean isShutdownComplete() {
		if(flushThread == null) {
			return true;
		}
		
		return ! flushThread.isAlive();
	}
	
	protected void createSSTableDirIfNeeded() {
		final File rootDir = new File(directory);		
		final File directoryHandle = new File(getSSTableDir(directory, name));
		
		if(rootDir.exists() && ! directoryHandle.exists()) {
			logger.info("Create a new dir for table: " + name);
			directoryHandle.mkdir();
		}
	}
	
	protected void scanForExistingTables() {
		logger.info("Scan for existing SSTables: " + name);
		final File directoryHandle = new File(getSSTableDir(directory, name));
		
		checkSSTableDir(directoryHandle);
		
		final File[] entries = directoryHandle.listFiles();
				
		for(final File file : entries) {
			final String filename = file.getName();
			if(isFileNameSSTable(filename)) {
				logger.info("Found sstable: " + filename);
				
				try {
					final SSTableReader reader = new SSTableReader(name, directory, file);
					tableReader.add(reader);
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
		
		for(AbstractTableReader reader : tableReader) {
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
	 */
	protected void checkSSTableDir(final File directoryHandle) {
		if(! directoryHandle.isDirectory()) {
			logger.error("Storage directory is not an directory: " + directoryHandle);
			storageState.setReady(false);
		}
	}
	
	/**
	 * Delete all existing SSTables in the given directory
	 * 
	 * @return Directory was deleted or not
	 */
	public boolean deleteExistingTables() {
		logger.info("Delete all existing SSTables for relation: " + name);
		final File directoryHandle = new File(getSSTableDir(directory, name));
	
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
		
		logger.debug("Searching for: " + key);
		
		Tuple tuple = null;
		
		// Read tuple from unflushed memtables
		for(final Memtable unflushedMemtable : unflushedMemtables) {
			tuple = unflushedMemtable.get(key);

			if(tuple != null) {
				return tuple;
			}
		}
		
		// Read data from the persistent SSTables
		for(final SSTableReader reader : tableReader) {

			final SSTableIndexReader indexReader = getIndexReaderForTable(reader);
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
		}
		
		if(tuple instanceof DeletedTuple) {
			return null;
		}
		
		return tuple;
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
	
	class SSTableFlusher implements Runnable {

		@Override
		public void run() {
			while(ready) {
				while(unflushedMemtables.isEmpty()) {
					try {					
						synchronized (unflushedMemtables) {
							unflushedMemtables.wait();
						}
					} catch (InterruptedException e) {
						logger.info("Memtable flush thread has stopped: " + name);
						return;
					}
				}
				
				// Flush all pending memtables to disk
				while(! unflushedMemtables.isEmpty()) {
					final Memtable memtable = unflushedMemtables.get(0);
					final File sstableFile = writeMemtable(memtable);
					
					if(sstableFile != null) {
						try {
							final SSTableReader reader = new SSTableReader(name, directory, sstableFile);
							tableReader.add(reader);
						} catch (StorageManagerException e) {
							logger.error("Exception while creating SSTable reader", e);
						}
					}
					
					final Memtable removedTable = unflushedMemtables.remove(0);

					if(memtable != removedTable) {
						logger.error("Get other table than removed!");
					}

				}
			}
			
			logger.info("Memtable flush thread has stopped: " + name);
		}
			
		/**
		 * Write a memtable to disk and return the Filehandle of the table
		 * 
		 * @param memtable
		 * @return
		 */
		protected File writeMemtable(final Memtable memtable) {
			logger.info("Writing new memtable: " + tableNumber);
			
			try(final SSTableWriter ssTableWriter = new SSTableWriter(directory, name, tableNumber)) {
				ssTableWriter.open();
				final File filehandle = ssTableWriter.getSstableFile();
				ssTableWriter.addData(memtable.getSortedTupleList());
				return filehandle;
			} catch (Exception e) {
				logger.info("Exception while write memtable: " + name + " / " + tableNumber, e);
				storageState.setReady(false);
			} finally {
				tableNumber++;
			}
			
			return null;
		}
	}

}
