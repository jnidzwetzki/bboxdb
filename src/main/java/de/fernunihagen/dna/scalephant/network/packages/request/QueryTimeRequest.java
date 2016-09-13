package de.fernunihagen.dna.scalephant.network.packages.request;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import de.fernunihagen.dna.scalephant.Const;
import de.fernunihagen.dna.scalephant.network.NetworkConst;
import de.fernunihagen.dna.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.scalephant.network.packages.NetworkQueryRequestPackage;
import de.fernunihagen.dna.scalephant.network.packages.PackageEncodeError;
import de.fernunihagen.dna.scalephant.network.routing.RoutingHeader;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;

public class QueryTimeRequest implements NetworkQueryRequestPackage {
	
	/**
	 * The name of the table
	 */
	protected final SSTableName table;

	/**
	 * The timestamp
	 */
	protected final long timestamp;

	public QueryTimeRequest(final String table, final long timestamp) {
		this.table = new SSTableName(table);
		this.timestamp = timestamp;
	}

	@Override
	public void writeToOutputStream(final short sequenceNumber, final OutputStream outputStream) throws PackageEncodeError {

		try {
			final byte[] tableBytes = table.getFullnameBytes();
			
			final ByteBuffer bb = ByteBuffer.allocate(14);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			
			bb.put(getQueryType());
			bb.put(NetworkConst.UNUSED_BYTE);  // Used byte 1
			bb.put(NetworkConst.UNUSED_BYTE);  // Used byte 2
			bb.put(NetworkConst.UNUSED_BYTE);  // Used byte 3
			bb.putLong(timestamp);
			bb.putShort((short) tableBytes.length);
			
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
			throw new PackageEncodeError("Got exception while converting package into bytes", e);
		}	
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PackageEncodeError 
	 */
	public static QueryTimeRequest decodeTuple(final ByteBuffer encodedPackage) throws PackageEncodeError {
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_QUERY);
		
		if(decodeResult == false) {
			throw new PackageEncodeError("Unable to decode package");
		}
		
	    final byte queryType = encodedPackage.get();
	    
	    if(queryType != NetworkConst.REQUEST_QUERY_TIME) {
	    	throw new PackageEncodeError("Wrong query type: " + queryType);
	    }
		
	    // 3 unused bytes
	    encodedPackage.get(); 
	    encodedPackage.get();
	    encodedPackage.get();
	    
	    final long timestmap = encodedPackage.getLong();
		final short tableLength = encodedPackage.getShort();
		
		final byte[] tableBytes = new byte[tableLength];
		encodedPackage.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeError("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new QueryTimeRequest(table, timestmap);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_QUERY;
	}

	@Override
	public byte getQueryType() {
		return NetworkConst.REQUEST_QUERY_TIME;
	}
	
	public SSTableName getTable() {
		return table;
	}

	public long getTimestamp() {
		return timestamp;
	}

}
