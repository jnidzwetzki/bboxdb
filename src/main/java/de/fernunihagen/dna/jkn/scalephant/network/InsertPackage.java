package de.fernunihagen.dna.jkn.scalephant.network;

import de.fernunihagen.dna.jkn.scalephant.storage.BoundingBox;

public class InsertPackage implements NetworkPackage {

	protected final String table;
	protected final String key;
	protected final long timestamp;
	protected final BoundingBox bbox;
	protected final String data;
	
	public InsertPackage(final String table, final String key, final long timestamp,
			final BoundingBox bbox, final String data) {
		super();
		this.table = table;
		this.key = key;
		this.timestamp = timestamp;
		this.bbox = bbox;
		this.data = data;
	}
	
	public InsertPackage(byte encodedPackage[]) {
		// FIXME:
		table = null;
		key = null;
		timestamp = 0;
		bbox = null;
		data = null;
	}

	@Override
	public void getByteArray() {
		// TODO Auto-generated method stub
		
	}

}
