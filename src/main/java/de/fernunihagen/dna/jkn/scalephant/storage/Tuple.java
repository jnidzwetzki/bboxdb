package de.fernunihagen.dna.jkn.scalephant.storage;

public class Tuple {
	
	protected String key;
	protected BoundingBox boundingBox;
	protected byte[] bytes;
	protected short seen;
	
	public Tuple(final String key, final BoundingBox boundingBox, final byte[] bytes) {
		super();
		this.key = key;
		this.boundingBox = boundingBox;
		this.bytes = bytes;
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
