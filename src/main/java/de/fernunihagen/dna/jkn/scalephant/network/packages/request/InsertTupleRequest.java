package de.fernunihagen.dna.jkn.scalephant.network.packages.request;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkRequestPackage;
import de.fernunihagen.dna.jkn.scalephant.storage.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableHelper;

public class InsertTupleRequest implements NetworkRequestPackage {

	/**
	 * The name of the table
	 */
	protected final String table;
	
	/**
	 * The Tuple
	 */
	protected Tuple tuple;
	
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
	public InsertTupleRequest(final String table, final Tuple tuple) {
		this.table = table;
		this.tuple = tuple;
	}
	
	/**
	 * Decode the encoded tuple into a object
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static InsertTupleRequest decodeTuple(final ByteBuffer encodedPackage) {

		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_INSERT_TUPLE);
		
		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		short tableLength = encodedPackage.getShort();
		short keyLength = encodedPackage.getShort();
		int bBoxLength = encodedPackage.getInt();
		int dataLength = encodedPackage.getInt();
		long timestamp = encodedPackage.getLong();
		
		final byte[] tableBytes = new byte[tableLength];
		encodedPackage.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		final byte[] keyBytes = new byte[keyLength];
		encodedPackage.get(keyBytes, 0, keyBytes.length);
		final String key = new String(keyBytes);
		
		final byte[] boxBytes = new byte[bBoxLength];
		encodedPackage.get(boxBytes, 0, boxBytes.length);

		final byte[] dataBytes = new byte[dataLength];
		encodedPackage.get(dataBytes, 0, dataBytes.length);

		if(encodedPackage.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + encodedPackage.remaining());
		}
		
		final long[] longArray = SSTableHelper.readLongArrayFromByte(boxBytes);
		final BoundingBox boundingBox = new BoundingBox(longArray);
		
		final Tuple tuple = new Tuple(key, boundingBox, dataBytes, timestamp);

		return new InsertTupleRequest(table, tuple);
	}

	@Override
	public byte[] getByteArray(final short sequenceNumber) {
		
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();
		
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForRequestPackage(sequenceNumber, getPackageType());
		
		try {
			final byte[] tableBytes = table.getBytes();
			final byte[] keyBytes = tuple.getKey().getBytes();
			final byte[] bboxBytes = tuple.getBoundingBoxBytes();
			
			final ByteBuffer bb = ByteBuffer.allocate(20);
			bb.order(NetworkConst.NETWORK_BYTEORDER);
			bb.putShort((short) tableBytes.length);
			bb.putShort((short) keyBytes.length);
			bb.putInt(bboxBytes.length);
			bb.putInt(tuple.getDataBytes().length);
			bb.putLong(tuple.getTimestamp());
			
			// Write body length
			final int bodyLength = bb.capacity() + tableBytes.length 
					+ keyBytes.length + bboxBytes.length + tuple.getDataBytes().length;
			
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(4);
			bodyLengthBuffer.order(NetworkConst.NETWORK_BYTEORDER);
			bodyLengthBuffer.putInt(bodyLength);
			bos.write(bodyLengthBuffer.array());
			
			// Write body
			bos.write(bb.array());
			bos.write(tableBytes);
			bos.write(keyBytes);
			bos.write(bboxBytes);
			bos.write(tuple.getDataBytes());
			
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

	public Tuple getTuple() {
		return tuple;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		result = prime * result + ((tuple == null) ? 0 : tuple.hashCode());
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
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		if (tuple == null) {
			if (other.tuple != null)
				return false;
		} else if (!tuple.equals(other.tuple))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "InsertTupleRequest [table=" + table + ", tuple=" + tuple + "]";
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_INSERT_TUPLE;
	}
}
