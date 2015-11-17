package de.fernunihagen.dna.jkn.scalephant.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableManager;

public class StorageManager implements Lifecycle, Storage {
	
	protected final String table;
	protected final StorageConfiguration configuration;
	protected final SSTableManager sstableManager;

	protected Memtable memtable;
	protected boolean ready;
	
	private final static Logger logger = LoggerFactory.getLogger(StorageManager.class);

	public StorageManager(final String table, final StorageConfiguration configuration) {
		super();
		this.table = table;
		this.configuration = configuration;

		this.sstableManager = new SSTableManager(table, 
				configuration.getDataDir());
		
		ready = false;
	}

	public void init() {
		logger.info("Initalize the storage manager for table: " + table);
		
		// Init the memtable before the sstablemanager. This ensures, that the
		// sstable recovery can put entries into the memtable
		initNewMemtable();
		sstableManager.init();
		
		ready = true;
	}

	public void shutdown() {
		ready = false;
		memtable.shutdown();
		sstableManager.shutdown();
	}
	
	private void initNewMemtable() {
		memtable = new Memtable(table, 
				configuration.getMemtableEntries(), 
				configuration.getMemtableSize());
		
		memtable.init();
	}
	
	@Override
	public void put(final Tuple tuple) throws StorageManagerException {
		if(! ready) {
			throw new StorageManagerException("Storage manager is not ready");
		}
		
		if(memtable.isFull()) {
			sstableManager.flushMemtable(memtable);
			initNewMemtable();
		}
		
		memtable.put(tuple);
	}

	@Override
	public Tuple get(final String key) throws StorageManagerException {
		
		if(! ready) {
			throw new StorageManagerException("Storage manager is not ready");
		}
		
		final Tuple tuple = memtable.get(key);
		
		if(tuple != null) {
			return tuple;
		}
		
		return sstableManager.get(key);
	}

	@Override
	public void delete(final String key) throws StorageManagerException {
		memtable.delete(key);
	}
	
	@Override
	public void clear() {
		memtable.clear();
	}
}