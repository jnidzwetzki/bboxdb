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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packets.NetworkResponsePacket;
import org.bboxdb.network.packets.PacketEncodeException;

public abstract class AbstractBodyResponse extends NetworkResponsePacket {

	/**
	 * The result body
	 */
	private final String body;

	public AbstractBodyResponse(final short sequenceNumber, final String body) {
		super(sequenceNumber);
		this.body = body;
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PacketEncodeException {
		
		try {
			final byte[] bodyBytes = body.getBytes();
			final ByteBuffer bb = ByteBuffer.allocate(2);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			bb.putShort((short) bodyBytes.length);
			
			// Write body length
			final long bodyLength = bb.capacity() + bodyBytes.length;			
			final long headerLength = appendResponsePackageHeader(bodyLength, outputStream);
	
			// Write body
			outputStream.write(bb.array());
			outputStream.write(bodyBytes);
			
			return headerLength;
		} catch (IOException e) {
			throw new PacketEncodeException("Got exception while converting package into bytes", e);
		}	
	}
	

	/**
	 * Decode the message of the answer
	 * @param bb
	 * @return
	 * @throws PacketEncodeException 
	 */
	protected static String decodeMessage(final ByteBuffer bb, final short packageType) throws PacketEncodeException {
		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(bb, packageType);

		if(decodeResult == false) {
			throw new PacketEncodeException("Unable to decode package");
		}
		
		final short bodyLength = bb.getShort();
		
		final byte[] bodyBytes = new byte[bodyLength];
		bb.get(bodyBytes, 0, bodyBytes.length);
		final String body = new String(bodyBytes);
		
		if(bb.remaining() != 0) {
			throw new PacketEncodeException("Some bytes are left after encoding: " + bb.remaining());
		}
		
		return body;
	}

	/**
	 * Get the message string
	 * @return
	 */
	public String getBody() {
		return body;
	}

}