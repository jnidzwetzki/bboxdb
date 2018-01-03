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
package org.bboxdb.network.packages;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bboxdb.misc.Const;

public abstract class NetworkResponsePackage extends NetworkPackage {

	public NetworkResponsePackage(final short sequenceNumber) {
		super(sequenceNumber);
	}

	/**
	 * Append the response package header to the output stream
	 * @param sequenceNumber
	 * @param packageType 
	 * @param bos
	 * @return length of the reader
	 * @throws PackageEncodeException 
	 */
	protected long appendResponsePackageHeader(final long bodyLength, final OutputStream bos) 
			throws PackageEncodeException {
		
		final ByteBuffer byteBuffer = ByteBuffer.allocate(12);
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
		byteBuffer.putShort(sequenceNumber);
		byteBuffer.putShort(getPackageType());
		byteBuffer.putLong(bodyLength);

		try {
			bos.write(byteBuffer.array());
		} catch (IOException e) {
			throw new PackageEncodeException(e);
		}
		
		return byteBuffer.capacity();
	}
}
