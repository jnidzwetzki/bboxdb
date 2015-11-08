package de.fernunihagen.dna.jkn.scalephant.storage;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;

public class StorageManager implements Lifecycle {
	
	protected final StorageConfiguration configuration;
	protected boolean ready;

	public StorageManager(final StorageConfiguration configuration) {
		super();
		this.configuration = configuration;
		ready = false;
	}

	public void init() {
		ready = true;
	}

	public void shutdown() {
		ready = false;
	}
	
	public void put(final int key, final Tuple value) {
		
	}
	
	public Tuple get(final int key) {
		return null;
	}
	
}
