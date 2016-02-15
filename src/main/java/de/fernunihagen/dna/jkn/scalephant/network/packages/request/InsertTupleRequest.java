package de.fernunihagen.dna.jkn.scalephant.network.packages.request;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkRequestPackage;
import de.fernunihagen.dna.jkn.scalephant.storage.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableHelper;

public class InsertTupleRequest implements NetworkRequestPackage {

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
	protected final byte[] data;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(InsertTupleRequest.class);
	
	
	/**
	 * Create package from parameter
	 * 
	 * @param table
	 * @param key
	 * @param timestamp
	 * @param bbox
	 * @param data
	 */
	public InsertTupleRequest(final String table, final String key, final long timestamp,
			final BoundingBox bbox, final byte[] data) {
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
	public static InsertTupleRequest decodeTuple(final byte encodedPackage[]) {

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_INSERT_TUPLE);
		
		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		short tableLength = bb.getShort();
		short keyLength = bb.getShort();
		int bBoxLength = bb.getInt();
		int dataLength = bb.getInt();
		long timestamp = bb.getLong();
		
		final byte[] tableBytes = new byte[tableLength];
		bb.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		final byte[] keyBytes = new byte[keyLength];
		bb.get(keyBytes, 0, keyBytes.length);
		final String key = new String(keyBytes);
		
		final byte[] boxBytes = new byte[bBoxLength];
		bb.get(boxBytes, 0, boxBytes.length);

		final byte[] dataBytes = new byte[dataLength];
		bb.get(dataBytes, 0, dataBytes.length);

		if(bb.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + bb.remaining());
		}
		
		final long[] longArray = SSTableHelper.readLongArrayFromByte(boxBytes);
		final BoundingBox boundingBox = new BoundingBox(longArray);

		return new InsertTupleRequest(table, key, timestamp, boundingBox, dataBytes);
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
	public byte[] getByteArray(final short sequenceNumber) {
		
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();
		
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForRequestPackage(sequenceNumber, getPackageType());
		
		try {
			final byte[] tableBytes = table.getBytes();
			final byte[] keyBytes = key.getBytes();
			final byte[] bboxBytes = bbox.toByteArray();
			
			final ByteBuffer bb = ByteBuffer.allocate(20);
			bb.order(NetworkConst.NETWORK_BYTEORDER);
			bb.putShort((short) tableBytes.length);
			bb.putShort((short) keyBytes.length);
			bb.putInt(bboxBytes.length);
			bb.putInt(data.length);
			bb.putLong(timestamp);
			
			// Write body length
			final int bodyLength = bb.capacity() + tableBytes.length 
					+ keyBytes.length + bboxBytes.length + data.length;
			
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(4);
			bodyLengthBuffer.order(NetworkConst.NETWORK_BYTEORDER);
			bodyLengthBuffer.putInt(bodyLength);
			bos.write(bodyLengthBuffer.array());
			
			// Write body
			bos.write(bb.array());
			bos.write(tableBytes);
			bos.write(keyBytes);
			bos.write(bboxBytes);
			bos.write(data);
			
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

	public byte[] getData() {
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
		result = prime * result + Arrays.hashCode(data);
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
		InsertTupleRequest other = (InsertTupleRequest) obj;
		if (bbox == null) {
			if (other.bbox != null)
				return false;
		} else if (!bbox.equals(other.bbox))
			return false;
		if (!Arrays.equals(data, other.data))
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
