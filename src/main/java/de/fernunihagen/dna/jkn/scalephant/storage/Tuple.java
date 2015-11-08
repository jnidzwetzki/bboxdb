package de.fernunihagen.dna.jkn.scalephant.storage;

public class Tuple {
	
	protected byte[] bytes;
	protected short seen;
	protected BoundingBox boundingBox;
	
	public Tuple(final byte[] bytes, final BoundingBox boundingBox) {
		super();
		this.bytes = bytes;
		this.boundingBox = boundingBox;
	}

	/**
	 * Returns the size of the tuple in byte
	 * 
	 * @return
	 */
	public int getSize() {
		int totalSize = 0;
		
		totalSize += bytes.length;
		
		if(boundingBox != null) {
			totalSize += boundingBox.getSize();
		}
		
		return totalSize;
	}

	public byte[] getBytes() {
		return bytes;
	}	

}
