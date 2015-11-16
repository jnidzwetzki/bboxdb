package de.fernunihagen.dna.jkn.scalephant.storage;

public class Memtable {
	
	protected final Tuple[] data;
	protected int freePos;
	
	/**
	 * Maximal number of entries keept in memory
	 */
	protected final int entries;
	
	/**
	 * Maximal size of memtable
	 */
	protected final int size;

	public Memtable(int entries, int size) {
		this.entries = entries;
		this.size = size;
		
		this.data = new Tuple[entries];
		freePos = 0;
	}
	
}
