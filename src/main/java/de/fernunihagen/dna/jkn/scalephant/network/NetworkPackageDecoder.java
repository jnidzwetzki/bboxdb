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
	 * Validate the package header
	 * 
	 * @param buffer
	 * @return true or false, depending of the integrity of the package
	 */
	public static boolean validatePackageHeader(final ByteBuffer bb) {
		
		// Reset position
		bb.position(0);
		
		// Buffer is to little to contain valid data
		if(bb.remaining() < 64) {
			return false;
		}
		
		// Check protocol version
		if(bb.get() != NetworkConst.PROTOCOL_VERSION) {
			return false;
		}
		
		// Check package type
		if(bb.get() != NetworkConst.REQUEST_TYPE_INSERT_TUPLE) {
			return false;
		}
		
		// Read request id
		bb.getShort();
		
		final int bodyLength = bb.getInt();
		
		return bb.remaining() == bodyLength;
	}
	
	/**
	 * Get the request id form a package
	 * @param bb
	 * @return the request id
	 */
	public static short getRequestIDFromPackage(final ByteBuffer bb) {
		// Reset position (Protocol, Request Type)
		bb.position(2);
		
		// Read request id
		return bb.getShort();
	}

}
