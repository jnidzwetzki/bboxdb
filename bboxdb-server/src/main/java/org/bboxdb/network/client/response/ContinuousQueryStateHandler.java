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
package org.bboxdb.network.client.response;

import java.nio.ByteBuffer;

import org.bboxdb.network.client.connection.BBoxDBConnection;
import org.bboxdb.network.client.future.network.NetworkOperationFuture;
import org.bboxdb.network.packets.PacketEncodeException;
import org.bboxdb.network.packets.response.ContinuousQueryStateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousQueryStateHandler implements ServerResponseHandler {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ContinuousQueryStateHandler.class);

	/**
	 * Handle success with body result
	 * @return 
	 */
	@Override
	public boolean handleServerResult(final BBoxDBConnection bBoxDBConnection, 
			final ByteBuffer encodedPackage, final NetworkOperationFuture future)
			throws PacketEncodeException {
		
		
		final ContinuousQueryStateResponse result = ContinuousQueryStateResponse.decodePackage(encodedPackage);
		
		if(logger.isDebugEnabled()) {
			logger.debug("Handle ContinuousQueryStateResponse package (seq={}) from={}", 
					result.getSequenceNumber(), bBoxDBConnection.getConnectionName());
		}
		
		if(future != null) {
			future.setOperationResult(result.getContinuousQueryServerState());
			future.fireCompleteEvent();
		}
		
		return true;
	}

}