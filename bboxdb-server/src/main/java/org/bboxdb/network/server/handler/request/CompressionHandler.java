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
package org.bboxdb.network.server.handler.request;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.CompressionEnvelopeRequest;
import org.bboxdb.network.server.ClientConnectionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressionHandler implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(CompressionHandler.class);
	
	@Override
	/**
	 * Handle compressed packages. Uncompress envelope and handle package
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) {
		
		try {
			final InputStream compressedDataStream = CompressionEnvelopeRequest.decodePackage(encodedPackage);
						
			while(compressedDataStream.available() > 0) {
				clientConnectionHandler.handleNextPackage(compressedDataStream);
			}
			
		} catch (IOException | PackageEncodeException e) {
			logger.error("Got an exception while handling compression", e);
		} 
		
		return true;
	}
}
