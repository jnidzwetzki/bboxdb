package de.fernunihagen.dna.jkn.scalephant.network;

import de.fernunihagen.dna.jkn.scalephant.storage.BoundingBox;

public class InsertPackage implements NetworkPackage {

	protected final String table;
	protected final String key;
	protected final long timestamp;
	protected final BoundingBox bbox;
	protected final String data;
	
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
		super();
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
	public byte[] getByteArray() {
		return null;
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

}
