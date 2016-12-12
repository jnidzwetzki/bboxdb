/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.bboxdb.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.NetworkPackageEncoder;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.PackageEncodeError;
import org.bboxdb.network.routing.RoutingHeader;

public class CompressionEnvelopeRequest implements NetworkRequestPackage {
	
	/**
	 * The package to encode
	 */
	protected NetworkRequestPackage networkRequestPackage;
	
	/**
	 * The compression type
	 */
	protected byte compressionType;

	public CompressionEnvelopeRequest(final NetworkRequestPackage networkRequestPackage, final byte compressionType) {
		this.networkRequestPackage = networkRequestPackage;
		this.compressionType = compressionType;
	}

	public void writeToOutputStream(final short sequenceNumber, final OutputStream outputStream) throws PackageEncodeError {
		try {
			if(compressionType != NetworkConst.COMPRESSION_TYPE_GZIP) {
				throw new PackageEncodeError("Unknown compression method: " + compressionType);
			}
			
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final OutputStream os = new GZIPOutputStream(baos);
			networkRequestPackage.writeToOutputStream(sequenceNumber, os);
			os.close();
			final byte[] compressedBytes = baos.toByteArray();
			
			final ByteBuffer bb = ByteBuffer.allocate(5);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			bb.putInt(compressedBytes.length);
			bb.put(compressionType);
			
			// Body length
			final long bodyLength = bb.capacity() + compressedBytes.length;

			// Unrouted package
			final RoutingHeader routingHeader = new RoutingHeader(false);
			NetworkPackageEncoder.appendRequestPackageHeader(sequenceNumber, bodyLength, 
					routingHeader, getPackageType(), outputStream);
			
			// Write body
			outputStream.write(bb.array());
			outputStream.write(compressedBytes);

		} catch (IOException e) {
			throw new PackageEncodeError("Got an IO Exception while writing compressed data");
		}
	}

	/**
	 * Decode the encoded package into a uncompressed byte stream 
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws IOException 
	 * @throws PackageEncodeError 
	 */
	public static byte[] decodePackage(final ByteBuffer encodedPackage) throws PackageEncodeError {
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_COMPRESSION);
		
		if(decodeResult == false) {
			throw new PackageEncodeError("Unable to decode package");
		}
		
		final int compressedDataLength = encodedPackage.getInt();
		final byte compressionType = encodedPackage.get();
		
		if(compressionType != NetworkConst.COMPRESSION_TYPE_GZIP) {
			throw new PackageEncodeError("Unknown compression type: " + compressionType);
		}
		
		if(compressedDataLength != encodedPackage.remaining()) {
			throw new PackageEncodeError("Remaning : " + encodedPackage.remaining() + " bytes. But compressed data should have: " + compressedDataLength + " bytes");
		}
		
		final byte[] compressedBytes = new byte[compressedDataLength];
		encodedPackage.get(compressedBytes, 0, compressedDataLength);
		
		ByteArrayOutputStream baos;
		try {
			final ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);
			final GZIPInputStream inputStream = new GZIPInputStream(bais);
			baos = new ByteArrayOutputStream();

			final byte[] buffer = new byte[10240];
			for (int length = 0; (length = inputStream.read(buffer)) > 0; ) {
				baos.write(buffer, 0, length);
			}

			inputStream.close();
			baos.close();
			return baos.toByteArray();
		} catch (IOException e) {
			throw new PackageEncodeError(e);
		}
		
	}
	
	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_COMPRESSION;
	}
	
}