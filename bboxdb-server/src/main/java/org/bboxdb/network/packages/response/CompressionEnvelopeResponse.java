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
package org.bboxdb.network.packages.response;

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
import org.bboxdb.network.packages.NetworkResponsePackage;
import org.bboxdb.network.packages.PackageEncodeException;

public class CompressionEnvelopeResponse extends NetworkResponsePackage {

	/**
	 * The compression type
	 */
	protected byte compressionType;
	
	/**
	 * The package to encode
	 */
	protected List<NetworkResponsePackage> networkResponsePackages;

	public CompressionEnvelopeResponse(final byte compressionType, 
			final List<NetworkResponsePackage> networkResponsePackages) {
		
		super((short) 0);

		this.compressionType = compressionType;
		this.networkResponsePackages = networkResponsePackages;
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.RESPONSE_TYPE_COMPRESSION;
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PackageEncodeException {

		try {
			if(compressionType != NetworkConst.COMPRESSION_TYPE_GZIP) {
				throw new PackageEncodeException("Unknown compression method: " + compressionType);
			}
			
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final OutputStream os = new GZIPOutputStream(baos);
			
			// Write packages
			for(final NetworkResponsePackage networkResponsePackage : networkResponsePackages) {
				networkResponsePackage.writeToOutputStream(os);
			}
			
			os.close();
			final byte[] compressedBytes = baos.toByteArray();
			
			// Header
			final ByteBuffer bb = ByteBuffer.allocate(4);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			bb.put(compressionType);
			bb.putShort((short) networkResponsePackages.size());
			
			// Body length
			final long bodyLength = bb.capacity() + compressedBytes.length;

			// Write body length
			final long headerLength = appendResponsePackageHeader(bodyLength, outputStream);
			
			// Write body
			outputStream.write(bb.array());
			outputStream.write(compressedBytes);
			
			return headerLength + bodyLength;
		} catch (IOException e) {
			throw new PackageEncodeException("Got exception while converting package into bytes", e);
		}
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	public static InputStream decodePackage(final ByteBuffer encodedPackage) throws PackageEncodeException {
		
		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(encodedPackage, NetworkConst.RESPONSE_TYPE_COMPRESSION);
		
		if(decodeResult == false) {
			throw new PackageEncodeException("Unable to decode package");
		}
		
		final byte compressionType = encodedPackage.get();
		
		if(compressionType != NetworkConst.COMPRESSION_TYPE_GZIP) {
			throw new PackageEncodeException("Unknown compression type: " + compressionType);
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
	
}
