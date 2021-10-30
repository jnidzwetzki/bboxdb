/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.network.packets.request;

import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packets.NetworkRequestPacket;
import org.bboxdb.network.packets.PacketEncodeException;

public class DisconnectRequest extends NetworkRequestPacket {
	
	public DisconnectRequest(final short sequenceNumber) {
		super(sequenceNumber);
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PacketEncodeException {

		try {
			// Write body length
			final long bodyLength = 0;
			
			final long headerLength = appendRequestPackageHeader(bodyLength, outputStream);
			
			return headerLength + bodyLength;
		} catch (Exception e) {
			throw new PacketEncodeException("Got exception while converting package into bytes", e);
		}
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PacketEncodeException 
	 */
	public static DisconnectRequest decodeTuple(final ByteBuffer encodedPackage) throws PacketEncodeException {
		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(encodedPackage);
		
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_DISCONNECT);
		
		if(decodeResult == false) {
			throw new PacketEncodeException("Unable to decode package");
		}
		
		if(encodedPackage.remaining() != 0) {
			throw new PacketEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new DisconnectRequest(sequenceNumber);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_DISCONNECT;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof DisconnectRequest) {
			return true;
		}
		
		return false;
	}

	@Override
	public String toString() {
		return "DisconnectRequest []";
	}
	
}
