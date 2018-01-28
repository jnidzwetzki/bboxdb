package org.bboxdb.storage.entity;

public class TupleEntityIdentifier implements EntityIdentifier {
	
	private final String key;
	private final long version;
	
	public TupleEntityIdentifier(final String key, final long version) {
		this.key = key;
		this.version = version;
	}

	public String getKey() {
		return key;
	}

	public long getVersion() {
		return version;
	}

	@Override
	public String toString() {
		return "TupleEntityIdentifier [key=" + key + ", version=" + version + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + (int) (version ^ (version >>> 32));
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
		TupleEntityIdentifier other = (TupleEntityIdentifier) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (version != other.version)
			return false;
		return true;
	}

}
