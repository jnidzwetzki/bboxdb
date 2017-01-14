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
package org.bboxdb.network.packages;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bboxdb.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class NetworkResponsePackage extends NetworkPackage {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(NetworkResponsePackage.class);

	public NetworkResponsePackage(final short sequenceNumber) {
		super(sequenceNumber);
	}

	/**
	 * Append the response package header to the output stream
	 * @param sequenceNumber
	 * @param packageType 
	 * @param bos
	 */
	protected void appendResponsePackageHeader(final short sequenceNumber, final long bodyLength,
			final short packageType, final OutputStream bos) {
		
		final ByteBuffer byteBuffer = ByteBuffer.allocate(12);
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
		byteBuffer.putShort(sequenceNumber);
		byteBuffer.putShort(packageType);
		byteBuffer.putLong(bodyLength);

		try {
			bos.write(byteBuffer.array());
		} catch (IOException e) {
			logger.error("Exception while writing", e);
		}
	}
}
