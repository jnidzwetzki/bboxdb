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
package org.bboxdb.network;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.bboxdb.network.packages.PackageEncodeException;

public class NetworkHelper {

	/**
	 * Uncompress the data in the byte array
	 * @param compressionType 
	 * @param compressedBytes
	 * @return
	 * @throws PackageEncodeException
	 */
	public static byte[] uncompressBytes(final byte compressionType, 
			final byte[] compressedBytes) throws PackageEncodeException {
		
		try {
			final ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);
			final GZIPInputStream inputStream = new GZIPInputStream(bais);
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();

			final byte[] buffer = new byte[10240];
			for (int length = 0; (length = inputStream.read(buffer)) > 0; ) {
				baos.write(buffer, 0, length);
			}

			inputStream.close();
			baos.close();
			
			return baos.toByteArray();
		} catch (IOException e) {
			throw new PackageEncodeException(e);
		}
	}

}
