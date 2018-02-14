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
package org.bboxdb.network.packages.request;

import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.PackageEncodeException;

public class CancelQueryRequest extends NetworkRequestPackage {
	
	/**
	 * The sequence of the query
	 */
	protected final short querySequenceNumber;
	
	public CancelQueryRequest(final short sequenceNumber, final short querySequenceNumber) {
		super(sequenceNumber);
		this.querySequenceNumber = querySequenceNumber;
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PackageEncodeException {

		try {
			final ByteBuffer bb = ByteBuffer.allocate(2);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			bb.putShort((short) querySequenceNumber);
			
			// Calculate body length
			final long bodyLength = bb.capacity();
		
			final long headerLength = appendRequestPackageHeader(bodyLength, outputStream);
			
			// Write body
			outputStream.write(bb.array());

			return headerLength + bodyLength;
		} catch (Exception e) {
			throw new PackageEncodeException("Got exception while converting package into bytes", e);
		}	
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PackageEncodeException 
	 */
	public static CancelQueryRequest decodeTuple(final ByteBuffer encodedPackage) throws PackageEncodeException {
		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(encodedPackage);
		
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_CANCEL_QUERY);

		if(decodeResult == false) {
			throw new PackageEncodeException("Unable to decode package");
		}
		
		final short packageSequence = encodedPackage.getShort();
		
		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new CancelQueryRequest(sequenceNumber, packageSequence);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_CANCEL_QUERY;
	}

	public short getQuerySequence() {
		return querySequenceNumber;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + querySequenceNumber;
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
		CancelQueryRequest other = (CancelQueryRequest) obj;
		if (querySequenceNumber != other.querySequenceNumber)
			return false;
		return true;
	}

}
