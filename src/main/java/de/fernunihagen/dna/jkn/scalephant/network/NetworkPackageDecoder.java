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
			return false;
		}
		
		// Check protocol version
		if(bb.get() != NetworkConst.PROTOCOL_VERSION) {
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
		final int bodyLength = bb.getInt();

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
			return false;
		}
		
		// Check protocol version
		if(bb.get() != NetworkConst.PROTOCOL_VERSION) {
			return false;
		}
		
		// Check package type
		if(bb.get() != packageType) {
			return false;
		}
		
		// Read request id
		bb.getShort();
		
		final int bodyLength = bb.getInt();
		
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
	public static int getBodyLengthFromRequestPackage(final ByteBuffer bb) {
		// Set positon
		bb.position(4);
		
		// Read the body length
		return bb.getInt();
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
	public static int getBodyLengthFromResponsePackage(final ByteBuffer bb) {
		// Set positon
		bb.position(4);
		
		// Read the body length
		return bb.getInt();
	}
}
