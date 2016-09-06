package de.fernunihagen.dna.scalephant.network;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.Const;
import de.fernunihagen.dna.scalephant.network.routing.RoutingHeader;
import de.fernunihagen.dna.scalephant.network.routing.RoutingHeaderParser;

public class NetworkPackageDecoder {
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(NetworkPackageDecoder.class);

	/**
	 * Encapsulate the encoded package into a byte buffer
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static ByteBuffer encapsulateBytes(final byte[] encodedPackage) {
		final ByteBuffer bb = ByteBuffer.wrap(encodedPackage);
		bb.order(Const.APPLICATION_BYTE_ORDER);
		return bb;
	}
	
	/**
	 * Validate the response package header
	 * 
	 * @param buffer
	 * @return true or false, depending of the integrity of the package
	 */
	public static boolean validateResponsePackageHeader(final ByteBuffer bb, final short packageType) {
		
		// Reset position
		bb.position(0);
		
		// Buffer is to short to contain valid data
		if(bb.remaining() < 12) {
			logger.warn("Package header is to small: " + bb.remaining());
			return false;
		}
		
		// Read request id
		bb.getShort();
		
		// Check package type
		final short readPackageType = bb.getShort();
		if(readPackageType != packageType) {
			logger.warn("Got wrong package type (" + readPackageType + " / " + packageType + ")");
			return false;
		}
		
		// Get the length of the body
		final long bodyLength = bb.getLong();
		
		return bb.remaining() == bodyLength;
	}
	
	/**
	 * Get the request id form a response package
	 * @param bb
	 * @return the request id
	 */
	public static short getRequestIDFromResponsePackage(final ByteBuffer bb) {
		// Reset position (Protocol, Request Type)
		bb.position(1);
		
		// Read request id
		return bb.getShort();
	}
	
	/**
	 * Validate the request package header
	 * 
	 * @param buffer
	 * @return true or false, depending of the integrity of the package
	 */
	public static boolean validateRequestPackageHeader(final ByteBuffer bb, final short packageType) {
		
		// Reset position
		bb.position(0);
		
		// Buffer is to short to contain valid data
		if(bb.remaining() < 12) {
			logger.warn("Package header is to small: " + bb.remaining());
			return false;
		}
		
		// Check package type
		final short readPackageType = bb.getShort();
		if(readPackageType != packageType) {
			logger.warn("Got wrong package type (got " + readPackageType + " / expected " + packageType + ")");
			return false;
		}
		
		// Read request id
		bb.getShort();
		
		// Read body lengh
		final long bodyLength = bb.getLong();
		
		// Read header
		RoutingHeaderParser.skipRoutingHeader(bb);
		
		return bb.remaining() == bodyLength;
	}
	
	/**
	 * Get the request id form a request package
	 * @param bb
	 * @return the request id
	 */
	public static short getRequestIDFromRequestPackage(final ByteBuffer bb) {
		// Reset position (Request Type)
		bb.position(2);
		
		// Read request id
		return bb.getShort();
	}
	
	/**
	 * Decode the routing header from package header
	 * @param bb
	 * @return
	 * @throws IOException 
	 */
	public static RoutingHeader getRoutingHeaderFromRequestPackage(final ByteBuffer bb) throws IOException {
		bb.position(12);
		return RoutingHeaderParser.decodeRoutingHeader(bb);
	}
	
	/**
	 * Read the body length from a request package header
	 * @param bb
	 * @return
	 */
	public static long getBodyLengthFromRequestPackage(final ByteBuffer bb) {
		// Set position
		bb.position(4);
		
		// Read the body length
		return bb.getLong();
	}
	
	/**
	 * Get the query type from a request package
	 * @param bb
	 * @return
	 */
	public static byte getQueryTypeFromRequest(final ByteBuffer bb) {
		// Set the position
		bb.position(4);
		RoutingHeaderParser.skipRoutingHeader(bb);
		bb.getLong();
		
		return bb.get();
	}
	
	/**
	 * Get the package type from a request package
	 * @param bb
	 * @return
	 */
	public static short getPackageTypeFromRequest(final ByteBuffer bb) {
		bb.position(0);
		
		return bb.getShort();
	}
	
	/**
	 * Get the package type from a result package
	 * @param bb
	 * @return
	 */
	public static short getPackageTypeFromResponse(final ByteBuffer bb) {
		bb.position(2);
		
		return bb.getShort();
	}
	
	/**
	 * Read the body length from a response package header
	 * @param bb
	 * @return
	 */
	public static long getBodyLengthFromResponsePackage(final ByteBuffer bb) {
		// Set position
		bb.position(4);
		
		// Read the body length
		return bb.getLong();
	}
}
