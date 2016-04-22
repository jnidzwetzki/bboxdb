package de.fernunihagen.dna.jkn.scalephant.network;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkPackageDecoder {
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(NetworkPackageDecoder.class);

	/**
	 * Encapsulate the encoded package into a bytebuffer
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static ByteBuffer encapsulateBytes(final byte[] encodedPackage) {
		final ByteBuffer bb = ByteBuffer.wrap(encodedPackage);
		bb.order(NetworkConst.NETWORK_BYTEORDER);
		return bb;
	}
	
	/**
	 * Validate the response package header
	 * 
	 * @param buffer
	 * @return true or false, depending of the integrity of the package
	 */
	public static boolean validateResponsePackageHeader(final ByteBuffer bb, final byte packageType) {
		
		// Reset position
		bb.position(0);
		
		// Buffer is to little to contain valid data
		if(bb.remaining() < 8) {
			logger.warn("Package header is to small: " + bb.remaining());
			return false;
		}
		
		// Check protocol version
		byte protocolVersion = bb.get();
		if(protocolVersion != NetworkConst.PROTOCOL_VERSION) {
			logger.warn("Got wrong protocol version: " + protocolVersion);
			return false;
		}
		
		// Read request id
		bb.getShort();
		
		// Check package type
		byte readPackageType = bb.get();
		if(readPackageType != packageType) {
			logger.warn("Got wrong package type (" + readPackageType + " / " + packageType + ")");
			return false;
		}
		
		// Get the length of the body
		final long bodyLength = bb.getLong();

		return bb.remaining() == bodyLength;
	}
	
	/**
	 * Get the request id form a reponse package
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
	public static boolean validateRequestPackageHeader(final ByteBuffer bb, final byte packageType) {
		
		// Reset position
		bb.position(0);
		
		// Buffer is to little to contain valid data
		if(bb.remaining() < 8) {
			logger.warn("Package header is to small: " + bb.remaining());
			return false;
		}
		
		// Check protocol version
		byte protocolVersion = bb.get();
		if(protocolVersion != NetworkConst.PROTOCOL_VERSION) {
			logger.warn("Got wrong protocol version: " + protocolVersion);
			return false;
		}
		
		// Check package type
		byte readPackageType = bb.get();
		if(readPackageType != packageType) {
			logger.warn("Got wrong package type (" + readPackageType + " / " + packageType + ")");
			return false;
		}
		
		// Read request id
		bb.getShort();
		
		final long bodyLength = bb.getLong();
		
		return bb.remaining() == bodyLength;
	}
	
	/**
	 * Get the request id form a request package
	 * @param bb
	 * @return the request id
	 */
	public static short getRequestIDFromRequestPackage(final ByteBuffer bb) {
		// Reset position (Protocol, Request Type)
		bb.position(2);
		
		// Read request id
		return bb.getShort();
	}
	
	/**
	 * Read the body length from a request package header
	 * @param bb
	 * @return
	 */
	public static long getBodyLengthFromRequestPackage(final ByteBuffer bb) {
		// Set positon
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
		bb.position(12);
		
		return bb.get();
	}
	
	/**
	 * Get the package type from a request package
	 * @param bb
	 * @return
	 */
	public static byte getPackageTypeFromRequest(final ByteBuffer bb) {
		bb.position(1);
		
		return bb.get();
	}
	
	/**
	 * Get the package type from a result package
	 * @param bb
	 * @return
	 */
	public static byte getPackageTypeFromResponse(final ByteBuffer bb) {
		bb.position(3);
		
		return bb.get();
	}
	
	/**
	 * Read the body length from a response package header
	 * @param bb
	 * @return
	 */
	public static long getBodyLengthFromResponsePackage(final ByteBuffer bb) {
		// Set positon
		bb.position(4);
		
		// Read the body length
		return bb.getLong();
	}
}
