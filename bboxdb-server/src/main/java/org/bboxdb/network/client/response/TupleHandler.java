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
package org.bboxdb.network.client.response;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.future.OperationFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.TupleResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleHandler implements ServerResponseHandler {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleHandler.class);

	/**
	 * Handle a single tuple as result
	 * @return 
	 */
	@Override
	public boolean handleServerResult(final BBoxDBClient bboxDBClient, 
			final ByteBuffer encodedPackage, final OperationFuture future)
			throws PackageEncodeException {
		
		if(logger.isDebugEnabled()) {
			logger.debug("Handle tuple package");
		}
		
		final TupleListFuture pendingCall = (TupleListFuture) future;
		final TupleResponse singleTupleResponse = TupleResponse.decodePackage(encodedPackage);
		final short sequenceNumber = singleTupleResponse.getSequenceNumber();
		
		// Tuple is part of a multi tuple result
		if(bboxDBClient.getResultBuffer().containsKey(sequenceNumber)) {
			bboxDBClient.getResultBuffer().get(sequenceNumber).add(singleTupleResponse.getTuple());
			
			// The removal of the future depends, if this is a one
			// tuple result or a multiple tuple result
			return false;
		}
		
		// Single tuple is returned
		if(pendingCall != null) {
			pendingCall.setOperationResult(0, Arrays.asList(singleTupleResponse.getTuple()));
			pendingCall.fireCompleteEvent();
		}
		
		return true;
	}

}
