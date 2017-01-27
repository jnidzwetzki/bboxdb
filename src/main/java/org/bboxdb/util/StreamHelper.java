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
package org.bboxdb.util;

import java.io.IOException;
import java.io.InputStream;

public class StreamHelper {

	/**
	 * Read exactly n bytes into the buffer
	 * @param buffer
	 * @param bytesToRead
	 * @throws IOException 
	 */
	public static void readExactlyBytes(final InputStream inputStream, 
			final byte[] buffer, final int offset, final int bytesToRead) throws IOException {
				
		if(buffer.length < bytesToRead) {
			throw new IllegalArgumentException("Unable to read " + bytesToRead + " bytes into a buffer with size " + buffer.length);
		}
		
		int totalReadBytes = 0;
		
		while(totalReadBytes < bytesToRead) {
			int bytesRead = inputStream.read(buffer, offset + totalReadBytes, (bytesToRead - totalReadBytes));
			
			if(bytesRead <= 0) {
				throw new IOException("Return code on read operation: " + bytesRead);
			}
			
			totalReadBytes = totalReadBytes + bytesRead;
		}	
	}
	
	/**
	 * The exactly n bytes in the input stream
	 * @param inputStream
	 * @param bytesToSkip
	 * @throws IOException
	 */
	public static void skipBytesExcactly(final InputStream inputStream, final int bytesToSkip) throws IOException {
		int totalSkipBytes = 0;
		
		while(totalSkipBytes < bytesToSkip) {
			int skippedBytes = (int) inputStream.skip((bytesToSkip - totalSkipBytes));
			totalSkipBytes = totalSkipBytes + skippedBytes;
		}
	}
	
}
