package de.fernunihagen.dna.jkn.scalephant.storage;

public class Tuple {
	
	protected byte[] data;
	

	/**
	 * Returns the size of the tuple in byte
	 * 
	 * @return
	 */
	public int getSize() {
		int totalSize = 0;
		
		totalSize += data.length;
		
		return totalSize;
	}

}
