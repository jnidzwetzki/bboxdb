package de.fernunihagen.dna.jkn.scalephant.storage;

import java.util.Arrays;

public class Tuple {
	
	protected String key;
	protected BoundingBox boundingBox;
	protected byte[] dataBytes;
	protected short seen;
	protected long timestamp;
	
	public Tuple(final String key, final BoundingBox boundingBox, final byte[] dataBytes) {
		super();
		this.key = key;
		this.boundingBox = boundingBox;
		this.dataBytes = dataBytes;
		
		this.timestamp = System.nanoTime();
	}
	
	public Tuple(final String key, final BoundingBox boundingBox, final byte[] dataBytes, long timestamp) {
		super();
		this.key = key;
		this.boundingBox = boundingBox;
		this.dataBytes = dataBytes;
	}

	/**
	 * Returns the size of the tuple in byte
	 * 
	 * @return
	 */
	public int getSize() {
		int totalSize = 0;
		
		if(dataBytes != null) {
			totalSize += dataBytes.length;
		}
		
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
	public byte[] getDataBytes() {
		return dataBytes;
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

	/**
	 * Get the bounding box of the tuple
	 * 
	 * @return
	 */
	public BoundingBox getBoundingBox() {
		return boundingBox;
	}
	
	/**
	 * Get the byte array of the bounding box
	 * @return
	 */
	public byte[] getBoundingBoxBytes() {
		return boundingBox.toByteArray();
	}
	
	@Override
	public String toString() {
		return "Tuple [key=" + key + ", boundingBox=" + boundingBox
				+ ", dataBytes=" + Arrays.toString(dataBytes) + ", seen=" + seen
				+ ", timestamp=" + timestamp + "]";
	}
	
}