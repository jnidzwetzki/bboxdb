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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packets.NetworkRequestPacket;
import org.bboxdb.network.packets.PacketEncodeException;
import org.bboxdb.storage.entity.TupleStoreName;

public class DeleteTableRequest extends NetworkRequestPacket {
	
	/**
	 * The name of the table
	 */
	private final TupleStoreName table;

	public DeleteTableRequest(final short sequenceNumber, final String table) {
		super(sequenceNumber);
		
		this.table = new TupleStoreName(table);
	}
	
	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PacketEncodeException {

		try {
			final byte[] tableBytes = table.getFullnameBytes();
			final ByteBuffer bb = DataEncoderHelper.shortToByteBuffer((short) tableBytes.length);

			// Body length
			final long bodyLength = bb.capacity() + tableBytes.length;
			
			final long headerLength = appendRequestPackageHeader(bodyLength, outputStream);

			// Write body
			outputStream.write(bb.array());
			outputStream.write(tableBytes);
			
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
	public static DeleteTableRequest decodeTuple(final ByteBuffer encodedPackage) throws PacketEncodeException {
		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(encodedPackage);
		
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_DELETE_TABLE);
		
		if(decodeResult == false) {
			throw new PacketEncodeException("Unable to decode package");
		}
		
		final short tableLength = encodedPackage.getShort();
		
		final byte[] tableBytes = new byte[tableLength];
		encodedPackage.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		if(encodedPackage.remaining() != 0) {
			throw new PacketEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new DeleteTableRequest(sequenceNumber, table);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_DELETE_TABLE;
	}

	public TupleStoreName getTable() {
		return table;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((table == null) ? 0 : table.hashCode());
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
		DeleteTableRequest other = (DeleteTableRequest) obj;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		return true;
	}

}
