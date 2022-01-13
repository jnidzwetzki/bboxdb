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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkHelper;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packets.NetworkRequestPacket;
import org.bboxdb.network.packets.PacketEncodeException;

public class CompressionEnvelopeRequest extends NetworkRequestPacket {
	
	/**
	 * The compression type
	 */
	private byte compressionType;

	/**
	 * The packages to encode
	 */
	private List<NetworkRequestPacket> networkRequestPackages;

	public CompressionEnvelopeRequest(final byte compressionType, 
			final List<NetworkRequestPacket> networkRequestPackages) {
		
		// Don't use a real sequence number
		super((short) 0);
		
		this.compressionType = compressionType;
		this.networkRequestPackages = networkRequestPackages;
	}

	public long writeToOutputStream(final OutputStream outputStream) throws PacketEncodeException {
		try {
			if(compressionType != NetworkConst.COMPRESSION_TYPE_GZIP) {
				throw new PacketEncodeException("Unknown compression method: " + compressionType);
			}
			
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final OutputStream os = new GZIPOutputStream(baos);
			
			// Write packages
			for(final NetworkRequestPacket networkRequestPackage : networkRequestPackages) {
				networkRequestPackage.writeToOutputStream(os);
			}
			
			os.close();
			final byte[] compressedBytes = baos.toByteArray();
			
			// Header
			final ByteBuffer bb = ByteBuffer.allocate(4);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			bb.put(compressionType);
			bb.putShort((short) networkRequestPackages.size());
			
			// Body length
			final long bodyLength = bb.capacity() + compressedBytes.length;

			final long headerLength = appendRequestPackageHeader(bodyLength, outputStream);
			
			// Write body
			outputStream.write(bb.array());
			outputStream.write(compressedBytes);
			
			return headerLength + bodyLength;
		} catch (IOException e) {
			throw new PacketEncodeException("Got an IO Exception while writing compressed data");
		}
	}

	/**
	 * Decode the encoded package into a uncompressed byte stream 
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws IOException 
	 * @throws PacketEncodeException 
	 */
	public static InputStream decodePackage(final ByteBuffer encodedPackage) throws PacketEncodeException {
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_COMPRESSION);
		
		if(decodeResult == false) {
			throw new PacketEncodeException("Unable to decode package");
		}
		
		final byte compressionType = encodedPackage.get();
		
		if(compressionType != NetworkConst.COMPRESSION_TYPE_GZIP) {
			throw new PacketEncodeException("Unknown compression type: " + compressionType);
		}
		
		// Skip 3 bytes - Header
		encodedPackage.getShort();
		encodedPackage.get();
		
		final byte[] compressedBytes = new byte[encodedPackage.remaining()];
		encodedPackage.get(compressedBytes, 0, encodedPackage.remaining());
		
		final byte[] uncompressedBytes = NetworkHelper.uncompressBytes(compressionType, compressedBytes);
		
		final ByteArrayInputStream bis = new ByteArrayInputStream(uncompressedBytes);
		
		return bis;
	}
	
	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_COMPRESSION;
	}

	/**
	 * Get the request packages
	 * @return
	 */
	public List<NetworkRequestPacket> getNetworkRequestPackages() {
		return networkRequestPackages;
	}
	
}