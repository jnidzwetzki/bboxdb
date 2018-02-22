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

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.capabilities.PeerCapabilities;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.PackageEncodeException;

public class HelloRequest extends NetworkRequestPackage {
	
	/**
	 * The supported protocol version
	 */
	protected final int protocolVersion;
	
	/**
	 * The peer capabilities (e.g. compression)
	 */
	protected final PeerCapabilities peerCapabilities;
	
	public HelloRequest(final short sequenceNumber, final int protocolVersion, 
			final PeerCapabilities peerCapabilities) {
		
		super(sequenceNumber);
		
		this.protocolVersion = protocolVersion;
		this.peerCapabilities = peerCapabilities;
	}
	
	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PackageEncodeException {

		try {
			final ByteBuffer bb = DataEncoderHelper.intToByteBuffer(protocolVersion);
			final byte[] peerCapabilitiesBytes = peerCapabilities.toByteArray();
			
			// Body length
			final long bodyLength = bb.capacity() + peerCapabilitiesBytes.length;
			
			final long headerLength = appendRequestPackageHeader(bodyLength, outputStream);

			outputStream.write(bb.array());
			outputStream.write(peerCapabilitiesBytes);
			
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
	public static HelloRequest decodeRequest(final ByteBuffer encodedPackage) throws PackageEncodeException {
		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(encodedPackage);
		
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_HELLO);
		
		if(decodeResult == false) {
			throw new PackageEncodeException("Unable to decode package");
		}
		
		final int protocolVersion = encodedPackage.getInt();
		final byte[] capabilityBytes = new byte[PeerCapabilities.CAPABILITY_BYTES];
		encodedPackage.get(capabilityBytes, 0, capabilityBytes.length);

		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		final PeerCapabilities peerCapabilities = new PeerCapabilities(capabilityBytes);
		
		return new HelloRequest(sequenceNumber, protocolVersion, peerCapabilities);
	}
	
	/**
	 * Get the capabilities
	 * @return
	 */
	public PeerCapabilities getPeerCapabilities() {
		return peerCapabilities;
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_HELLO;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((peerCapabilities == null) ? 0 : peerCapabilities.hashCode());
		result = prime * result + protocolVersion;
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
		HelloRequest other = (HelloRequest) obj;
		if (peerCapabilities == null) {
			if (other.peerCapabilities != null)
				return false;
		} else if (!peerCapabilities.equals(other.peerCapabilities))
			return false;
		if (protocolVersion != other.protocolVersion)
			return false;
		return true;
	}

}
