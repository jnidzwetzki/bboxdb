package de.fernunihagen.dna.jkn.scalephant.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;

public class Memtable implements Lifecycle {
	
	protected final Tuple[] data;
	protected int freePos;
	
	private final static Logger logger = LoggerFactory.getLogger(Memtable.class);
	
	/**
	 * Maximal number of entries keept in memory
	 */
	protected final int entries;
	
	/**
	 * Maximal size of memtable in kb
	 */
	protected final int size;

	public Memtable(int entries, int size) {
		this.entries = entries;
		this.size = size;
		
		this.data = new Tuple[entries];
		freePos = -1;
	}

	@Override
	public void init() {
		if(freePos != -1) {
			logger.error("init() called on an initalized memtable");
			return;
		}
		
		logger.info("Initializing a new memtable");
		freePos = 0;
	}

	@Override
	public void shutdown() {
		
	}
	
}
