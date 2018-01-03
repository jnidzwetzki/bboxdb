/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
package org.bboxdb.network;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.bboxdb.misc.Const;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHeaderParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkPackageDecoder {
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(NetworkPackageDecoder.class);

	/**
	 * Encapsulate the encoded package into a byte buffer
	 * 
	 * @param bytes
	 * @return
	 */
	public static ByteBuffer encapsulateBytes(final byte[] bytes) {
		return encapsulateBytes(bytes, 0);
	}
	
	/**
	 * Encapsulate the encoded package into a byte buffer - with offset
	 * 
	 * @param bytes
	 * @return
	 */
	public static ByteBuffer encapsulateBytes(final byte[] bytes, final int offset) {
		final ByteBuffer bb = ByteBuffer.wrap(bytes, offset, bytes.length - offset);
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
			logger.warn("Got wrong package type (got {}, expected {})",  readPackageType, packageType);
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
		// Reset position (Request Type)
		bb.position(0);
		
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
		
		// Read request id
		bb.getShort();
		
		// Check package type
		final short readPackageType = bb.getShort();
		if(readPackageType != packageType) {
			logger.warn("Got wrong package type (got {}, expected {})",  readPackageType, packageType);
			return false;
		}
		
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
		bb.position(0);
		
		// Read request id
		return bb.getShort();
	}
	
	/**
	 * Decode the routing header from package header
	 * @param bb
	 * @return
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	public static RoutingHeader getRoutingHeaderFromRequestPackage(final ByteBuffer bb) throws IOException, PackageEncodeException {
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
		bb.position(12);
		RoutingHeaderParser.skipRoutingHeader(bb);
		return bb.get();
	}
	
	/**
	 * Get the package type from a request package
	 * @param bb
	 * @return
	 */
	public static short getPackageTypeFromRequest(final ByteBuffer bb) {
		bb.position(2);
		
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
