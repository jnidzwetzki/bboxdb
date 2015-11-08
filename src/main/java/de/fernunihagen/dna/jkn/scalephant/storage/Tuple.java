package de.fernunihagen.dna.jkn.scalephant.storage;

public class Tuple {
	
	protected byte[] data;
	protected BoundingBox boundingBox;
	

	/**
	 * Returns the size of the tuple in byte
	 * 
	 * @return
	 */
	public int getSize() {
		int totalSize = 0;
		
		totalSize += data.length;
		
		if(boundingBox != null) {
			totalSize += boundingBox.getSize();
		}
		
		return totalSize;
	}

}
