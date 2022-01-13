/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb.network.packets.response;

import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packets.NetworkResponsePacket;
import org.bboxdb.network.packets.PacketEncodeException;

public class MultipleTupleEndResponse extends NetworkResponsePacket {

	public MultipleTupleEndResponse(final short sequenceNumber) {
		super(sequenceNumber);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.RESPONSE_TYPE_MULTIPLE_TUPLE_END;
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PacketEncodeException {
		return appendResponsePackageHeader(0, outputStream);
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PacketEncodeException 
	 */
	public static MultipleTupleEndResponse decodePackage(final ByteBuffer encodedPackage) throws PacketEncodeException {
		
		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(encodedPackage, NetworkConst.RESPONSE_TYPE_MULTIPLE_TUPLE_END);
		
		if(decodeResult == false) {
			throw new PacketEncodeException("Unable to decode package");
		}
		
		if(encodedPackage.remaining() != 0) {
			throw new PacketEncodeException("Some bytes are left after encoding: " + encodedPackage.remaining());
		}
		
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);
		
		return new MultipleTupleEndResponse(requestId);
	}

}
