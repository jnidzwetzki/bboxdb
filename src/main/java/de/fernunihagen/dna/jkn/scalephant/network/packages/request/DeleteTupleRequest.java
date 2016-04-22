package de.fernunihagen.dna.jkn.scalephant.network.packages.request;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkRequestPackage;


public class DeleteTupleRequest implements NetworkRequestPackage {

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
	private final static Logger logger = LoggerFactory.getLogger(DeleteTupleRequest.class);
	
	public DeleteTupleRequest(final String table, final String key) {
		this.table = table;
		this.key = key;
	}

	/**
	 * Get the a encoded version of this class
	 */
	@Override
	public void writeToOutputStream(final short sequenceNumber,
			final OutputStream outputStream) {
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();
		
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForRequestPackage(sequenceNumber, getPackageType());
		
		try {
			final byte[] tableBytes = table.getBytes();
			final byte[] keyBytes = key.getBytes();
			
			final ByteBuffer bb = ByteBuffer.allocate(4);
			bb.order(NetworkConst.NETWORK_BYTEORDER);
			bb.putShort((short) tableBytes.length);
			bb.putShort((short) keyBytes.length);
			
			// Write body length
			final long bodyLength = bb.capacity() + tableBytes.length 
					+ keyBytes.length;
			
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(8);
			bodyLengthBuffer.order(NetworkConst.NETWORK_BYTEORDER);
			bodyLengthBuffer.putLong(bodyLength);
			bos.write(bodyLengthBuffer.array());
			
			// Write body
			bos.write(bb.array());
			bos.write(tableBytes);
			bos.write(keyBytes);
			bos.close();
			
			final byte[] outputData = bos.toByteArray();
			outputStream.write(outputData, 0, outputData.length);
		} catch (IOException e) {
			logger.error("Got exception while converting package into bytes", e);
		}
	}
	
	/**
	 * Decode the encoded package into an object
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static DeleteTupleRequest decodeTuple(final ByteBuffer encodedPackage) {
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_DELETE_TUPLE);
		
		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		final short tableLength = encodedPackage.getShort();
		final short keyLength = encodedPackage.getShort();
		
		final byte[] tableBytes = new byte[tableLength];
		encodedPackage.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		final byte[] keyBytes = new byte[keyLength];
		encodedPackage.get(keyBytes, 0, keyBytes.length);
		final String key = new String(keyBytes);

		if(encodedPackage.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + encodedPackage.remaining());
		}
		
		return new DeleteTupleRequest(table, key);
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
		DeleteTupleRequest other = (DeleteTupleRequest) obj;
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
