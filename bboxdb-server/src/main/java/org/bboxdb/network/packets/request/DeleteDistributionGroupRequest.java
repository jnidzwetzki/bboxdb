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
package org.bboxdb.network.packets.request;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packets.NetworkRequestPacket;
import org.bboxdb.network.packets.PacketEncodeException;

public class DeleteDistributionGroupRequest extends NetworkRequestPacket {
	
	/**
	 * The name of the table
	 */
	private final String distributionGroup;

	public DeleteDistributionGroupRequest(final short sequenceNumber, final String distributionGroup) {
		super(sequenceNumber);
		this.distributionGroup = distributionGroup;
	}
	
	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PacketEncodeException {

		try {
			final byte[] groupBytes = distributionGroup.getBytes();
			final ByteBuffer bb = DataEncoderHelper.shortToByteBuffer((short) groupBytes.length);
			
			// Body length
			final long bodyLength = bb.capacity() + groupBytes.length;
			
			final long headerLength = appendRequestPackageHeader(bodyLength, outputStream);

			// Write body
			outputStream.write(bb.array());
			outputStream.write(groupBytes);
			
			return headerLength + bodyLength;
		} catch (IOException e) {
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
	public static DeleteDistributionGroupRequest decodeTuple(final ByteBuffer encodedPackage) throws PacketEncodeException {
		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(encodedPackage);
		
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_DELETE_DISTRIBUTION_GROUP);
		
		if(decodeResult == false) {
			throw new PacketEncodeException("Unable to decode package");
		}
		
		final short groupLength = encodedPackage.getShort();
		
		final byte[] groupBytes = new byte[groupLength];
		encodedPackage.get(groupBytes, 0, groupBytes.length);
		final String distributionGroup = new String(groupBytes);
		
		if(encodedPackage.remaining() != 0) {
			throw new PacketEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new DeleteDistributionGroupRequest(sequenceNumber, distributionGroup);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_DELETE_DISTRIBUTION_GROUP;
	}

	public String getDistributionGroup() {
		return distributionGroup;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((distributionGroup == null) ? 0 : distributionGroup.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DeleteDistributionGroupRequest other = (DeleteDistributionGroupRequest) obj;
		if (distributionGroup == null) {
			if (other.distributionGroup != null)
				return false;
		} else if (!distributionGroup.equals(other.distributionGroup))
			return false;
		return true;
	}

}
