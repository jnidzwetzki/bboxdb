package de.fernunihagen.dna.jkn.scalephant.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;

public class Memtable implements Lifecycle, Storage {
	
	/**
	 * The name of the coresponding table
	 */
	protected final String table;
	
	/**
	 * The memtable
	 */
	protected final Tuple[] data;
	
	/**
	 * The next free position in the data array
	 */
	protected int freePos;
	
	/**
	 * Maximal number of entries keept in memory
	 */
	protected final int entries;
	
	/**
	 * Maximal size of memtable in kb
	 */
	protected final int size;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(Memtable.class);
	
	public Memtable(final String table, int entries, int size) {
		this.table = table;
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
		
		logger.info("Initializing a new memtable for table: " + table);
		freePos = 0;
	}

	@Override
	public void shutdown() {
		
	}

	@Override
	public void put(final Tuple value) {
		data[freePos] = value;
		freePos++;
	}

	/**
	 * Get the most recent version of the tuple for key
	 * 
	 */
	@Override
	public Tuple get(final String key) {
		
		Tuple tuple = null;
		
		for(int i = 0; i < freePos; i++) {
			final Tuple possibleTuple = data[i];
			if(possibleTuple != null && possibleTuple.getKey().equals(key)) {
				
				if(tuple == null) {
					tuple = possibleTuple;
				} else {
					if(tuple.getTimestamp() < possibleTuple.getTimestamp()) {
						tuple = possibleTuple;
					}
				}
			}
		}
		
		return tuple;
	}

	@Override
	public void clear() {
		logger.info("Clear on memtable " + table + " called");
		
		for(int i = 0; i < data.length; i++) {
			data[i] = null;
		}
		
		freePos = 0;
	}
	
}
