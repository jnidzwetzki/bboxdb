package de.fernunihagen.dna.jkn.scalephant.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;

public class StorageManager implements Lifecycle, Storage {
	
	protected final String table;
	protected final StorageConfiguration configuration;
	protected final Memtable memtable;
	protected boolean ready;
	
	private final static Logger logger = LoggerFactory.getLogger(StorageManager.class);

	public StorageManager(final String table, final StorageConfiguration configuration) {
		super();
		this.table = table;
		this.configuration = configuration;
		this.memtable = new Memtable(table, 
				configuration.getMemtableEntries(), 
				configuration.getMemtableSize());
		
		ready = false;
	}

	public void init() {
		logger.info("Initalize the storage manager for table: " + table);
		memtable.init();
		ready = true;
	}

	public void shutdown() {
		ready = false;
		memtable.shutdown();
	}
	
	@Override
	public void put(final Tuple tuple) throws StorageManagerException {
		if(! ready) {
			throw new StorageManagerException("Storage manager is not ready");
		}
		
		memtable.put(tuple);
	}

	@Override
	public Tuple get(final int key) throws StorageManagerException {
		
		if(! ready) {
			throw new StorageManagerException("Storage manager is not ready");
		}
		
		final Tuple tuple = memtable.get(key);
		
		if(tuple != null) {
			return tuple;
		}
		
		// Read data from the persistent SSTables
		return null;
	}

	@Override
	public void clear() {
		memtable.clear();
	}
	
}
