package de.fernunihagen.dna.jkn.scalephant.network.packages.request;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkRequestPackage;
import de.fernunihagen.dna.jkn.scalephant.network.routing.RoutingHeader;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.jkn.scalephant.tools.DataEncoderHelper;

public class DeleteTableRequest implements NetworkRequestPackage {
	
	/**
	 * The name of the table
	 */
	protected final SSTableName table;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DeleteTableRequest.class);
	
	public DeleteTableRequest(final String table) {
		this.table = new SSTableName(table);
	}
	
	@Override
	public void writeToOutputStream(final short sequenceNumber, final OutputStream outputStream) {

		try {
			final byte[] tableBytes = table.getFullnameBytes();
			final ByteBuffer bb = DataEncoderHelper.shortToByteBuffer((short) tableBytes.length);

			// Body length
			final long bodyLength = bb.capacity() + tableBytes.length;
			
			// Unrouted package
			final RoutingHeader routingHeader = new RoutingHeader(false);
			NetworkPackageEncoder.appendRequestPackageHeader(sequenceNumber, bodyLength, routingHeader, 
					getPackageType(), outputStream);
			
			// Write body
			outputStream.write(bb.array());
			outputStream.write(tableBytes);			
		} catch (IOException e) {
			logger.error("Got exception while converting package into bytes", e);
		}
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static DeleteTableRequest decodeTuple(final ByteBuffer encodedPackage) {
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_DELETE_TABLE);
		
		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		final short tableLength = encodedPackage.getShort();
		
		final byte[] tableBytes = new byte[tableLength];
		encodedPackage.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		if(encodedPackage.remaining() != 0) {
			logger.error("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new DeleteTableRequest(table);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_DELETE_TABLE;
	}

	public SSTableName getTable() {
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
		DeleteTableRequest other = (DeleteTableRequest) obj;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		return true;
	}

}
