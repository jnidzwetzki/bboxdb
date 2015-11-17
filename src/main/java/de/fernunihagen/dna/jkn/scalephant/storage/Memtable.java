package de.fernunihagen.dna.jkn.scalephant.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

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
	protected final int maxsize;
	
	/**
	 * Current size in kb
	 */
	protected int currentSize;
	

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(Memtable.class);
	
	public Memtable(final String table, int entries, int maxsize) {
		this.table = table;
		this.entries = entries;
		this.maxsize = maxsize;
		
		this.data = new Tuple[entries];
		freePos = -1;
		currentSize = 0;
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
	public void put(final Tuple value) throws StorageManagerException {
		
		if(freePos >= entries) {
			throw new StorageManagerException("Unable to store a new tuple, all memtable slots are full");
		}
		
		data[freePos] = value;
		freePos++;
		currentSize = currentSize + value.getSize();
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
		
		// The most recent tuple is the delete marker
		if(tuple instanceof DeletedTuple) {
			return null;
		}
		
		return tuple;
	}
	
	/**
	 * Delete a tuple, this is implemented by inserting a DeletedTuple object
	 *
	 */
	@Override
	public void delete(final String key) throws StorageManagerException {
		final Tuple deleteTuple = new DeletedTuple(key);
		put(deleteTuple);
	}
	

	/**
	 * Get a sorted list with all recent tuples
	 * @return 
	 * 
	 */
	public List<Tuple> getSortedTupleList() {
		final SortedMap<String, Tuple> allTuples = new TreeMap<String, Tuple>();
		
		for(int i = 0; i < freePos; i++) {
			final String key = data[i].getKey();
			
			if(! allTuples.containsKey(key)) {
				allTuples.put(key, data[i]);
			} else {
				if(allTuples.get(key).getTimestamp() < data[i].getTimestamp()) {
					
					if(data[i] instanceof DeletedTuple) {
						allTuples.remove(key);
					} else {
						allTuples.put(key, data[i]);
					}
				}
			}
		}
		
		final List<Tuple> resultList = new ArrayList<Tuple>(allTuples.size());
		resultList.addAll(allTuples.values());
		
		return resultList;
	}
	
	/**
	 * Clean the whole memtable, useful for testing
	 * 
	 */
	@Override
	public void clear() {
		logger.info("Clear on memtable " + table + " called");
		
		for(int i = 0; i < data.length; i++) {
			data[i] = null;
		}
		
		freePos = 0;
	}
	
	/**
	 * Is this memtable full and needs to be flushed to disk
	 * 
	 * @return
	 */
	public boolean isFull() {
		
		// Check size of the table
		if(currentSize >= maxsize) {
			return true;
		}
		
		// Check number of entries
		if(freePos + 1 >= entries) {
			return true;
		}
		
		return false;
	}
}
