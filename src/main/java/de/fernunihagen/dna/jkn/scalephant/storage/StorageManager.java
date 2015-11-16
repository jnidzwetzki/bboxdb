package de.fernunihagen.dna.jkn.scalephant.storage;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;

public class StorageManager implements Lifecycle, Storage {
	
	protected final StorageConfiguration configuration;
	protected final Memtable memtable;
	
	protected boolean ready;

	public StorageManager(final StorageConfiguration configuration) {
		super();
		this.configuration = configuration;
		this.memtable = new Memtable(configuration.getMemtableEntries(), configuration.getMemtableSize());
		
		ready = false;
	}

	public void init() {
		memtable.init();
		ready = true;
	}

	public void shutdown() {
		ready = false;
		memtable.shutdown();
	}
	
	@Override
	public void put(final Tuple tuple) {
		memtable.put(tuple);
	}

	@Override
	public Tuple get(final int key) {
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
