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
		ready = true;
	}

	public void shutdown() {
		ready = false;
	}
	
	@Override
	public void put(final int key, final Tuple value) {
		
	}

	@Override
	public Tuple get(final int key) {
		return null;
	}

	
}
