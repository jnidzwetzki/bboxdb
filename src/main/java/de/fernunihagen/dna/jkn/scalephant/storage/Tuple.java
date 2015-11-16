package de.fernunihagen.dna.jkn.scalephant.storage;

import java.util.Arrays;

public class Tuple {
	
	protected String key;
	protected BoundingBox boundingBox;
	protected byte[] bytes;
	protected short seen;
	protected long timestamp;
	
	public Tuple(final String key, final BoundingBox boundingBox, final byte[] bytes) {
		super();
		this.key = key;
		this.boundingBox = boundingBox;
		this.bytes = bytes;
		
		this.timestamp = System.nanoTime();
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

	/**
	 * Get the data of the tuple
	 * 
	 * @return
	 */
	public byte[] getBytes() {
		return bytes;
	}
	
	/**
	 * Get the timestamp of the tuple
	 * 
	 * @return
	 */
	public long getTimestamp() {
		return timestamp;
	}
	
	/**
	 * Get the key of the tuple
	 * 
	 * @return
	 */
	public String getKey() {
		return key;
	}

	@Override
	public String toString() {
		return "Tuple [key=" + key + ", boundingBox=" + boundingBox
				+ ", bytes=" + Arrays.toString(bytes) + ", seen=" + seen
				+ ", timestamp=" + timestamp + "]";
	}
	
}