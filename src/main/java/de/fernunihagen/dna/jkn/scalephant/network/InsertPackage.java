package de.fernunihagen.dna.jkn.scalephant.network;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.BoundingBox;

public class InsertPackage implements NetworkPackage {

	/**
	 * The name of the table
	 */
	protected final String table;
	
	/**
	 * The key to insert
	 */
	protected final String key;
	
	/**
	 * The timestamp of the tuple
	 */
	protected final long timestamp;
	
	/**
	 * The bounding box of the tuple
	 */
	protected final BoundingBox bbox;
	
	/**
	 * The data 
	 */
	protected final String data;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(InsertPackage.class);
	
	
	/**
	 * Create package from parameter
	 * 
	 * @param table
	 * @param key
	 * @param timestamp
	 * @param bbox
	 * @param data
	 */
	public InsertPackage(final String table, final String key, final long timestamp,
			final BoundingBox bbox, final String data) {
		this.table = table;
		this.key = key;
		this.timestamp = timestamp;
		this.bbox = bbox;
		this.data = data;
	}
	
	/**
	 * Create package from byte string
	 * 
	 * @param encodedPackage
	 */
	public InsertPackage(byte encodedPackage[]) {
		// FIXME:
		table = null;
		key = null;
		timestamp = 0;
		bbox = null;
		data = null;
	}

	@Override
	public byte[] getByteArray(final SequenceNumberGenerator sequenceNumberGenerator) {
		
		final NetworkPackageBuilder networkPackageBuilder 
			= new NetworkPackageBuilder(sequenceNumberGenerator);
		
		final ByteArrayOutputStream bos = networkPackageBuilder.getByteOutputStream(getPackageType());
		
		try {
			final byte[] bboxBytes = bbox.toByteArray();
			final byte[] dataBytes = data.getBytes();
			bos.write(table.getBytes());
			bos.write(key.getBytes().length);
			bos.write(bboxBytes.length);
			bos.write(dataBytes.length);
						
			bos.close();
		} catch (IOException e) {
			logger.error("Got exception while converting package into bytes", e);
			return null;
		}
		
		return bos.toByteArray();
	}

	
	public String getTable() {
		return table;
	}

	public String getKey() {
		return key;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public BoundingBox getBbox() {
		return bbox;
	}

	public String getData() {
		return data;
	}

	@Override
	public String toString() {
		return "InsertPackage [table=" + table + ", key=" + key
				+ ", timestamp=" + timestamp + ", bbox=" + bbox + ", data="
				+ data + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bbox == null) ? 0 : bbox.hashCode());
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		InsertPackage other = (InsertPackage) obj;
		if (bbox == null) {
			if (other.bbox != null)
				return false;
		} else if (!bbox.equals(other.bbox))
			return false;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		if (timestamp != other.timestamp)
			return false;
		return true;
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_INSERT_TUPLE;
	}
}
