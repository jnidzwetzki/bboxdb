package de.fernunihagen.dna.jkn.scalephant.network;

import java.nio.ByteBuffer;

public class NetworkPackageDecoder {
	
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
		if(bb.remaining() < 4) {
			return false;
		}
		
		// Check protocol version
		if(bb.get() != NetworkConst.PROTOCOL_VERSION) {
			return false;
		}
		
		// Read request id
		bb.getShort();
		
		// Check package type
		if(bb.get() != packageType) {
			return false;
		}
		
		return true;
	}
	
	/**
	 * Get the request id form a reponse package
	 * @param bb
	 * @return the request id
	 */
	public static short geRequestIDFromResponsePackage(final ByteBuffer bb) {
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
	 * Read the body length from a package header
	 * @param bb
	 * @return
	 */
	public static int getBodyLengthFromRequestPackage(final ByteBuffer bb) {
		// Set positon
		bb.position(4);
		
		// Read the body length
		return bb.getInt();
	}

}
