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
package org.bboxdb.network.client.response;

import java.nio.ByteBuffer;
import java.util.List;

import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.future.OperationFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.MultipleTupleEndResponse;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultipleTupleEndHandler implements ServerResponseHandler {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(MultipleTupleEndHandler.class);
	

	
	@Override
	public boolean handleServerResult(final BBoxDBClient bboxDBClient, 
			final ByteBuffer encodedPackage, final OperationFuture future)
			throws PackageEncodeException {
		
		if(logger.isDebugEnabled()) {
			logger.debug("Handle multuple tuple end package");
		}
		
		final TupleListFuture pendingCall = (TupleListFuture) future;
		
		final MultipleTupleEndResponse result = MultipleTupleEndResponse.decodePackage(encodedPackage);
		
		final short sequenceNumber = result.getSequenceNumber();
		final List<Tuple> resultList = bboxDBClient.getResultBuffer().remove(sequenceNumber);

		if(pendingCall == null) {
			logger.warn("Got handleMultiTupleEnd and pendingCall is empty (package {}) ",
					sequenceNumber);
			return true;
		}
		
		if(resultList == null) {
			logger.warn("Got handleMultiTupleEnd and resultList is empty (package {})",
					sequenceNumber);
			
			pendingCall.setFailedState();
			pendingCall.fireCompleteEvent();
			return true;
		}
		
		pendingCall.setCompleteResult(0, true);
		pendingCall.setOperationResult(0, resultList);
		pendingCall.fireCompleteEvent();
		
		return true;
	}
}
