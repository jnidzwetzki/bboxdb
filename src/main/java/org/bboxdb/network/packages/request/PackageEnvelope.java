/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.bboxdb.BBoxDBConfiguration;
import org.bboxdb.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageEncoder;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.PackageEncodeError;
import org.bboxdb.network.routing.RoutingHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PackageEnvelope implements NetworkRequestPackage {

	protected final List<NetworkRequestPackage> packages;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(PackageEnvelope.class);

	public PackageEnvelope(final List<NetworkRequestPackage> packages) {
		this.packages = packages;
	}

	/**
	 * Get the a encoded version of this class
	 * @throws PackageEncodeError 
	 */
	@Override
	public void writeToOutputStream(final short sequenceNumber, final OutputStream outputStream) throws PackageEncodeError {

		try {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			
			for(final NetworkRequestPackage networkRequestPackage : packages) {
				networkRequestPackage.writeToOutputStream(sequenceNumber, baos);
			}
			
			baos.close();
			final byte[] bytes = baos.toByteArray();
			
			final ByteBuffer bb = ByteBuffer.allocate(4);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			bb.putInt(packages.size());
			
			// Body length
			final long bodyLength = bb.capacity() + bytes.length;

			// Unrouted package
			final RoutingHeader routingHeader = new RoutingHeader(false);
			NetworkPackageEncoder.appendRequestPackageHeader(sequenceNumber, bodyLength, 
					routingHeader, getPackageType(), outputStream);
			
			// Write body
			outputStream.write(bb.array());
			outputStream.write(bytes);
			
		} catch (IOException e) {
			throw new PackageEncodeError("Got exception while converting package into bytes", e);
		}		
	}


	
	/**
	 * Decode the encoded package into an object
	 * 
	 * @param encodedPackage
	 * @param bodyLength 
	 * @param inputStream 
	 * @return
	 * @throws IOException 
	 * @throws PackageEncodeError 
	 */
	public static boolean decodeTuple(final ByteBuffer packageHeader, final long bodyLength, 
			final BBoxDBConfiguration configuration, final InputStream inputStream) throws IOException, PackageEncodeError {
		
		// TODO:
		
		return true;
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_ENVELOPE;
	}
	
	/**
	 * Get the stored packages
	 * @return
	 */
	public List<NetworkRequestPackage> getPackages() {
		return packages;
	}

}
