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
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableManager.class);

	public SSTableManager(final String name, final String directory) {
		super();
		this.name = name;
		this.directory = directory;
		
		tableNumber = 0;
		unflushedMemtables = new CopyOnWriteArrayList<Memtable>();
	}

	@Override
	public void init() {
		logger.info("Init a new instance for the table: " + name);
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
	
	protected static String getSSTableFilename(final String directoy, final String name, int tablebumber) {
		return directoy 
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
				
				final Memtable memtable = unflushedMemtables.remove(0);
				writeMemtable(memtable);
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
			} finally {
				tableNumber++;
			}
		}
	}

}
