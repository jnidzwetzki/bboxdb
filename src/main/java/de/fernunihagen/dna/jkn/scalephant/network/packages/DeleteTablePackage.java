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

public class DeleteTablePackage implements NetworkRequestPackage {
	
	/**
	 * The name of the table
	 */
	protected final String table;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DeleteTablePackage.class);
	
	public DeleteTablePackage(final String table) {
		this.table = table;
	}
	

	@Override
	public byte[] getByteArray(SequenceNumberGenerator sequenceNumberGenerator) {
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder(sequenceNumberGenerator);
	
		final ByteArrayOutputStream bos = networkPackageEncoder.getByteOutputStream(getPackageType());
		
		try {
			final byte[] tableBytes = table.getBytes();
			
			final ByteBuffer bb = ByteBuffer.allocate(2);
			bb.order(NetworkConst.NETWORK_BYTEORDER);
			bb.putShort((short) tableBytes.length);
			
			// Write body length
			final int bodyLength = bb.capacity() + tableBytes.length;
			
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(4);
			bodyLengthBuffer.order(NetworkConst.NETWORK_BYTEORDER);
			bodyLengthBuffer.putInt(bodyLength);
			bos.write(bodyLengthBuffer.array());
			
			// Write body
			bos.write(bb.array());
			bos.write(tableBytes);
			
			bos.close();
		} catch (IOException e) {
			logger.error("Got exception while converting package into bytes", e);
			return null;
		}
	
		return bos.toByteArray();
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static DeleteTablePackage decodeTuple(final byte encodedPackage[]) {
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		NetworkPackageDecoder.validatePackageHeader(bb, NetworkConst.REQUEST_TYPE_DELETE_TABLE);
		
		short tableLength = bb.getShort();
		
		final byte[] tableBytes = new byte[tableLength];
		bb.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		if(bb.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + bb.remaining());
		}
		
		return new DeleteTablePackage(table);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_DELETE_TABLE;
	}

	public String getTable() {
		return table;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
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
		DeleteTablePackage other = (DeleteTablePackage) obj;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		return true;
	}

}
