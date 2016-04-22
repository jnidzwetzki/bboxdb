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
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;

public class QueryBoundingBoxRequest implements NetworkQueryRequestPackage {
	
	/**
	 * The name of the table
	 */
	protected final String table;

	/**
	 * The the query bounding box
	 */
	protected final BoundingBox box;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(QueryBoundingBoxRequest.class);
	
	public QueryBoundingBoxRequest(final String table, final BoundingBox box) {
		this.table = table;
		this.box = box;
	}

	@Override
	public byte[] getByteArray(final short sequenceNumber) {
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();
	
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForRequestPackage(sequenceNumber, getPackageType());
		
		try {
			final byte[] tableBytes = table.getBytes();
			final byte[] bboxBytes = box.toByteArray();
			
			final ByteBuffer bb = ByteBuffer.allocate(6);
			bb.order(NetworkConst.NETWORK_BYTEORDER);
			
			bb.put(getQueryType());
			bb.put(NetworkConst.UNUSED_BYTE);
			bb.putShort((short) tableBytes.length);
			bb.putShort((short) bboxBytes.length);
			
			// Write body length
			final long bodyLength = bb.capacity() + tableBytes.length + bboxBytes.length;
			
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(8);
			bodyLengthBuffer.order(NetworkConst.NETWORK_BYTEORDER);
			bodyLengthBuffer.putLong(bodyLength);
			bos.write(bodyLengthBuffer.array());
			
			// Write body
			bos.write(bb.array());
			bos.write(tableBytes);
			bos.write(bboxBytes);
			
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
	public static QueryBoundingBoxRequest decodeTuple(final ByteBuffer encodedPackage) {
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_QUERY);
		
		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
	    final byte queryType = encodedPackage.get();
	    
	    if(queryType != NetworkConst.REQUEST_QUERY_BBOX) {
	    	logger.error("Wrong query type: " + queryType + " required type is: " + NetworkConst.REQUEST_QUERY_BBOX);
	    	return null;
	    }
	    
	    // 1 unused byte
	    encodedPackage.get();
		
		final short tableLength = encodedPackage.getShort();
		final short bboxLength = encodedPackage.getShort();
		
		final byte[] tableBytes = new byte[tableLength];
		encodedPackage.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		final byte[] bboxBytes = new byte[bboxLength];
		encodedPackage.get(bboxBytes, 0, bboxBytes.length);
		final BoundingBox boundingBox = BoundingBox.fromByteArray(bboxBytes);
		
		if(encodedPackage.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + encodedPackage.remaining());
		}
		
		return new QueryBoundingBoxRequest(table, boundingBox);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_QUERY;
	}

	@Override
	public byte getQueryType() {
		return NetworkConst.REQUEST_QUERY_BBOX;
	}
	
	public String getTable() {
		return table;
	}

	public BoundingBox getBoundingBox() {
		return box;
	}

}
