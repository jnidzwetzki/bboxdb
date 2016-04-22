package de.fernunihagen.dna.jkn.scalephant.network.packages.request;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkQueryRequestPackage;

public class QueryTimeRequest implements NetworkQueryRequestPackage {
	
	/**
	 * The name of the table
	 */
	protected final String table;

	/**
	 * The timestamp
	 */
	protected final long timestamp;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(QueryTimeRequest.class);
	
	public QueryTimeRequest(final String table, final  long timestamp) {
		this.table = table;
		this.timestamp = timestamp;
	}

	@Override
	public byte[] getByteArray(final short sequenceNumber) {
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();
	
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForRequestPackage(sequenceNumber, getPackageType());
		
		try {
			final byte[] tableBytes = table.getBytes();
			
			final ByteBuffer bb = ByteBuffer.allocate(14);
			bb.order(NetworkConst.NETWORK_BYTEORDER);
			
			bb.put(getQueryType());
			bb.put("0".getBytes()); // Used byte 1
			bb.put("0".getBytes()); // Used byte 2
			bb.put("0".getBytes()); // Used byte 3
			bb.putLong(timestamp);
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
	public static QueryTimeRequest decodeTuple(final ByteBuffer encodedPackage) {
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_QUERY);
		
		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
	    final byte queryType = encodedPackage.get();
	    
	    if(queryType != NetworkConst.REQUEST_QUERY_TIME) {
	    	logger.error("Wrong query type: " + queryType);
	    	return null;
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
			logger.error("Some bytes are left after encoding: " + encodedPackage.remaining());
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
	
	public String getTable() {
		return table;
	}

	public long getTimestamp() {
		return timestamp;
	}

}
