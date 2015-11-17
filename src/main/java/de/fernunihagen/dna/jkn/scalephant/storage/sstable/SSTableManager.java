package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;
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
	}

	@Override
	public void init() {
		logger.info("Init a new instance for the table: " + name);
		createSSTableDirIfNeeded();
		scanForExistingTables();
		
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
					final int sequenceNumber = extractSequenceFromFilename(filename);
					
					if(sequenceNumber >= tableNumber) {
						tableNumber = sequenceNumber + 1;
					}
					
				} catch(StorageManagerException e) {
					logger.warn("Unable to parse sequence number, ignoring file: " + filename, e);
				}
			}
		}
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
		return filename.startsWith(SSTableConst.FILE_PREFIX) 
				&& filename.endsWith(SSTableConst.FILE_SUFFIX);
	}

	/**
	 * Extract the sequence Number from a given filename
	 * 
	 * @param filename
	 * @return the sequence number
	 * @throws StorageManagerException 
	 */
	protected int extractSequenceFromFilename(final String filename) throws StorageManagerException {
		try {
			final String sequence = filename
				.replace(SSTableConst.FILE_PREFIX + name + "_", "")
				.replace(SSTableConst.FILE_SUFFIX, "");
		
			return Integer.parseInt(sequence);
		
		} catch (NumberFormatException e) {
			String error = "Unable to parse sequence number: " + filename;
			logger.warn(error);
			throw new StorageManagerException(error, e);
		}
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
	
	public Tuple get(final String key) throws StorageManagerException {
		
		Tuple tuple;
		
		// Read tuple from unflushed memtables
		for(final Memtable unflushedMemtable : unflushedMemtables) {
			tuple = unflushedMemtable.get(key);

			if(tuple != null) {
				return tuple;
			}
		}
		
		// TODO: Read data from the persistent SSTables
		
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
	 * The full name of the SSTable file for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/scalephant/data/relation1/sstable_relation1_2.sst
	 */
	protected static String getSSTableFilename(final String directory, final String name, int tablebumber) {
		return getSSTableDir(directory, name)
				+ File.separator 
				+ SSTableConst.FILE_PREFIX 
				+ name 
				+ "_" 
				+ tablebumber 
				+ SSTableConst.FILE_SUFFIX;
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
					final Memtable memtable = unflushedMemtables.remove(0);
					writeMemtable(memtable);
				}
			}
			
			logger.info("Memtable flush thread has stopped: " + name);
		}
			
		protected void writeMemtable(final Memtable memtable) {
			logger.info("Writing new memtable");
			
			try(final SSTableWriter ssTableWriter = new SSTableWriter(directory, name, tableNumber)) {
				ssTableWriter.open();
				ssTableWriter.addData(memtable.getSortedTupleList());
			} catch (Exception e) {
				logger.info("Exception while write memtable: " + name + " / " + tableNumber, e);
				storageState.setReady(false);
			} finally {
				tableNumber++;
			}
		}
	}

}
