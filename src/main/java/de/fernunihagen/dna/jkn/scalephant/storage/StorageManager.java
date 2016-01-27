package de.fernunihagen.dna.jkn.scalephant.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableManager;
import de.fernunihagen.dna.jkn.scalephant.util.State;

public class StorageManager implements Lifecycle, Storage {
	
	protected final String table;
	protected final StorageConfiguration configuration;
	protected final SSTableManager sstableManager;
	protected final State state;

	protected Memtable memtable;
	
	private final static Logger logger = LoggerFactory.getLogger(StorageManager.class);

	public StorageManager(final String table, final StorageConfiguration configuration) {
		super();
		this.table = table;
		this.configuration = configuration;
		this.state = new State(false);
		this.sstableManager = new SSTableManager(state, table, configuration.getDataDir());
	}

	public void init() {
		logger.info("Initalize the storage manager for table: " + table);
		
		// Init the memtable before the sstablemanager. This ensures, that the
		// sstable recovery can put entries into the memtable
		initNewMemtable();
		sstableManager.init();
		
		state.setReady(true);
	}

	public void shutdown() {
		state.setReady(false);
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
		
		if(! state.isReady()) {
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
		
		if(! state.isReady()) {
			throw new StorageManagerException("Storage manager is not ready");
		}
		
		final Tuple tuple = memtable.get(key);
		
		if(tuple instanceof DeletedTuple) {
			return null;
		}
		
		if(tuple != null) {
			return tuple;
		}

		return sstableManager.get(key);
	}

	@Override
	public void delete(final String key) throws StorageManagerException {
		
		if(! state.isReady()) {
			throw new StorageManagerException("Storage manager is not ready");
		}
		
		memtable.delete(key);
	}
	
	/**
	 * Clear all entries in the table
	 * 
	 * 1) Reject new writes to this table 
	 * 2) Clear the memtable
	 * 3) Shutdown the sstable flush service
	 * 4) Wait for shutdown complete
	 * 5) Delete all persistent sstables
	 * 6) Restart the service
	 * 7) Accept new writes
	 * 
	 */
	@Override
	public void clear() {
		shutdown();
		
		memtable.clear();
		
		while(! sstableManager.isShutdownComplete()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				return;
			}
		}
		
		try {
			sstableManager.deleteExistingTables();
		} catch (StorageManagerException e) {
			logger.error("Error during deletion", e);
		}
		
		init();
	}
	
	/**
	 * Returns if the storage manager is ready or not
	 * @return
	 */
	public boolean isReady() {
		
		if(sstableManager.isReady() == false) {
			return false;
		}
		
		return state.isReady();
	}
}