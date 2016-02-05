package de.fernunihagen.dna.jkn.scalephant.network;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.BoundingBox;

public class InsertTuplePackage implements NetworkPackage {

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
	private final static Logger logger = LoggerFactory.getLogger(InsertTuplePackage.class);
	
	
	/**
	 * Create package from parameter
	 * 
	 * @param table
	 * @param key
	 * @param timestamp
	 * @param bbox
	 * @param data
	 */
	public InsertTuplePackage(final String table, final String key, final long timestamp,
			final BoundingBox bbox, final String data) {
		this.table = table;
		this.key = key;
		this.timestamp = timestamp;
		this.bbox = bbox;
		this.data = data;
	}
	
	/**
	 * Decode the encoded tuple into a object
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static InsertTuplePackage decodeTuple(final byte encodedPackage[]) {

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		NetworkPackageDecoder.validatePackageHeader(bb);
		
		return null;
	}
	
	/**
	 * Check validity of the entries
	 * @return
	 */
	protected boolean isValied() {
		if(table.getBytes().length > 16) {
			logger.warn("Tablename to long: " + table);
			return false;
		}
		
		return true;
	}

	@Override
	public byte[] getByteArray(final SequenceNumberGenerator sequenceNumberGenerator) {
		
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder(sequenceNumberGenerator);
		
		final ByteArrayOutputStream bos = networkPackageEncoder.getByteOutputStream(getPackageType());
		
		try {
			final byte[] tableBytes = table.getBytes();
			final byte[] keyBytes = key.getBytes();
			final byte[] bboxBytes = bbox.toByteArray();
			final byte[] dataBytes = data.getBytes();
			
			final ByteBuffer bb = ByteBuffer.allocate(160);
			bb.order(NetworkConst.NETWORK_BYTEORDER);
			bb.putShort((short) tableBytes.length);
			bb.putShort((short) keyBytes.length);
			bb.putInt(bboxBytes.length);
			bb.putInt(dataBytes.length);
			
			// Write body length
			final int bodyLength = bb.capacity() + tableBytes.length 
					+ keyBytes.length + bboxBytes.length + dataBytes.length;
			
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(4);
			bodyLengthBuffer.order(NetworkConst.NETWORK_BYTEORDER);
			bodyLengthBuffer.putInt(bodyLength);
			bos.write(bodyLengthBuffer.array());
			
			// Write body
			bos.write(bb.array());
			bos.write(tableBytes);
			bos.write(keyBytes);
			bos.write(bboxBytes);
			bos.write(dataBytes);
			
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
		InsertTuplePackage other = (InsertTuplePackage) obj;
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
