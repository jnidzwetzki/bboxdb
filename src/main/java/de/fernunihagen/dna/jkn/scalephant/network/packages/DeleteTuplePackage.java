package de.fernunihagen.dna.jkn.scalephant.network.packages;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.SequenceNumberGenerator;


public class DeleteTuplePackage implements NetworkRequestPackage {

	/**
	 * The name of the table
	 */
	protected final String table;
	
	/**
	 * The key to delete
	 */
	protected final String key;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DeleteTuplePackage.class);
	
	public DeleteTuplePackage(final String table, final String key) {
		this.table = table;
		this.key = key;
	}

	/**
	 * Get the a encoded version of this class
	 */
	@Override
	public byte[] getByteArray(SequenceNumberGenerator sequenceNumberGenerator) {
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();
		
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForRequestPackage(sequenceNumberGenerator, getPackageType());
		
		try {
			final byte[] tableBytes = table.getBytes();
			final byte[] keyBytes = key.getBytes();
			
			final ByteBuffer bb = ByteBuffer.allocate(4);
			bb.order(NetworkConst.NETWORK_BYTEORDER);
			bb.putShort((short) tableBytes.length);
			bb.putShort((short) keyBytes.length);
			
			// Write body length
			final int bodyLength = bb.capacity() + tableBytes.length 
					+ keyBytes.length;
			
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(4);
			bodyLengthBuffer.order(NetworkConst.NETWORK_BYTEORDER);
			bodyLengthBuffer.putInt(bodyLength);
			bos.write(bodyLengthBuffer.array());
			
			// Write body
			bos.write(bb.array());
			bos.write(tableBytes);
			bos.write(keyBytes);
			
			bos.close();
		} catch (IOException e) {
			logger.error("Got exception while converting package into bytes", e);
			return null;
		}
		
		return bos.toByteArray();
	}
	
	/**
	 * Decode the encoded package into an object
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static DeleteTuplePackage decodeTuple(final byte encodedPackage[]) {
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_DELETE_TUPLE);
		
		short tableLength = bb.getShort();
		short keyLength = bb.getShort();
		
		final byte[] tableBytes = new byte[tableLength];
		bb.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		final byte[] keyBytes = new byte[keyLength];
		bb.get(keyBytes, 0, keyBytes.length);
		final String key = new String(keyBytes);

		if(bb.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + bb.remaining());
		}
		
		return new DeleteTuplePackage(table, key);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_DELETE_TUPLE;
	}

	public String getTable() {
		return table;
	}

	public String getKey() {
		return key;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((table == null) ? 0 : table.hashCode());
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
		DeleteTuplePackage other = (DeleteTuplePackage) obj;
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
		return true;
	}

}
