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
package org.bboxdb.network.routing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.util.DataEncoderHelper;

import com.google.common.io.ByteStreams;

public class RoutingHeaderParser {

	/**
	 * Decode the routing header from a input stream
	 * @param bb
	 * @return
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	public static RoutingHeader decodeRoutingHeader(final InputStream inputStream) throws IOException, PackageEncodeException {
		
		final byte[] routedOrDirect = new byte[1];		
		ByteStreams.readFully(inputStream, routedOrDirect, 0, routedOrDirect.length);
		
		if(        (routedOrDirect[0] != RoutingHeader.DIRECT_PACKAGE) 
				&& (routedOrDirect[0] != RoutingHeader.ROUTED_PACKAGE)) {
			
			throw new PackageEncodeException("Invalid package routing type, unable to decode package "
					+ "header: " + routedOrDirect);
		}
		
		if(routedOrDirect[0] == RoutingHeader.DIRECT_PACKAGE) {
			return decodeDirectPackage(inputStream);
		} else {
			return decodeRoutedPackage(inputStream);
		} 
	}
	
	/**
	 * Read the routing header from a byte buffer
	 * @param bb
	 * @return
	 * @throws IOException
	 * @throws PackageEncodeException 
	 */
	public static RoutingHeader decodeRoutingHeader(final ByteBuffer bb) throws IOException, PackageEncodeException {
		final ByteArrayInputStream bis = new ByteArrayInputStream(bb.array(), bb.position(), bb.remaining());
				
		final RoutingHeader routingHeader = decodeRoutingHeader(bis);
		skipRoutingHeader(bb);
		return routingHeader;
	}
	
	/**
	 * Skip the bytes of the routing header
	 * @param bb
	 */
	public static void skipRoutingHeader(final ByteBuffer bb) {
		bb.get(); 		// Routed or direct
		bb.getShort(); 	// Hop
		bb.get(); 		// Unused
		final short routingListLength = bb.getShort();	// Routing list length		
		bb.position(bb.position() + routingListLength);		
	}
	
	/**
	 * Decode a routed package header
	 * @param inputStream
	 * @return
	 * @throws IOException
	 */
	protected static RoutingHeader decodeRoutedPackage(final InputStream inputStream) throws IOException {
		
		// Hop
		final byte[] hopBuffer = new byte[2];
		ByteStreams.readFully(inputStream, hopBuffer, 0, hopBuffer.length);
		final short hop = DataEncoderHelper.readShortFromByte(hopBuffer);
		
		// Skip one unused byte
		ByteStreams.skipFully(inputStream, 1);

		// Routing list list length
		final byte[] routingListLengthBuffer = new byte[2];
		ByteStreams.readFully(inputStream, routingListLengthBuffer, 0, routingListLengthBuffer.length);
		final short routingListLength = DataEncoderHelper.readShortFromByte(routingListLengthBuffer);

		final byte[] routingListBuffer = new byte[routingListLength];
		ByteStreams.readFully(inputStream, routingListBuffer, 0, routingListBuffer.length);
		final String routingList = new String(routingListBuffer);
		
		return new RoutingHeader(hop, routingList);
	}

	/**
	 * Decode a direct package header
	 * @param inputStream
	 * @return
	 * @throws IOException
	 */
	protected static RoutingHeader decodeDirectPackage(final InputStream inputStream) throws IOException {
		// Skip 5 unused bytes
		ByteStreams.skipFully(inputStream, 5);
		return new RoutingHeader(false);
	}

	/**
	 * Encode the routing header into a byte buffer
	 * @throws IOException 
	 */
	public static byte[] encodeHeader(final RoutingHeader routingHeader) throws IOException {
		final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		
		if(routingHeader.isRoutedPackage()) {
			byteArrayOutputStream.write(RoutingHeader.ROUTED_PACKAGE);
			
			// Hop
			final ByteBuffer hop = DataEncoderHelper.shortToByteBuffer(routingHeader.getHop());
			byteArrayOutputStream.write(hop.array());
			
			// Unused
			byteArrayOutputStream.write(0x00);
			
			// Length of routing list
			final String routingList = routingHeader.getRoutingListAsString();
			final ByteBuffer rountingListLength = DataEncoderHelper.shortToByteBuffer((short) routingList.length());
			byteArrayOutputStream.write(rountingListLength.array());
			
			// Host list
			byteArrayOutputStream.write(routingList.getBytes());
			
		} else {
			byteArrayOutputStream.write(RoutingHeader.DIRECT_PACKAGE);
			
			// Hop
			byteArrayOutputStream.write(0x00);
			byteArrayOutputStream.write(0x00);
			
			// Unused
			byteArrayOutputStream.write(0x00);
			
			// Length of routing list
			byteArrayOutputStream.write(0x00);
			byteArrayOutputStream.write(0x00);
		}
		
		return byteArrayOutputStream.toByteArray();
	}
}
