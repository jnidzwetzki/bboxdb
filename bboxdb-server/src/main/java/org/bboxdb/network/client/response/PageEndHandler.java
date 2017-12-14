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
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.future.OperationFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.PageEndResponse;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageEndHandler implements ServerResponseHandler {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(PageEndHandler.class);

	/**
	 * Handle the end of a page
	 * @return 
	 */
	@Override
	public boolean handleServerResult(final BBoxDBClient bboxDBClient, 
			final ByteBuffer encodedPackage, final OperationFuture future)
			throws PackageEncodeException {
		
		if(logger.isDebugEnabled()) {
			logger.debug("Handle page end package");
		}
		
		final TupleListFuture pendingCall = (TupleListFuture) future;
		
		if(pendingCall == null) {
			logger.warn("Got handleMultiTupleEnd and pendingCall is empty");
			return true;
		}

		final PageEndResponse result = PageEndResponse.decodePackage(encodedPackage);
		final short sequenceNumber = result.getSequenceNumber();

		final List<Tuple> resultList = bboxDBClient.getResultBuffer().remove(sequenceNumber);
		
		// Collect tuples of the next page in new list
		bboxDBClient.getResultBuffer().put(sequenceNumber, new ArrayList<Tuple>());
		
		if(resultList == null) {
			logger.warn("Got handleMultiTupleEnd and resultList is empty");
			pendingCall.setFailedState();
			pendingCall.fireCompleteEvent();
			return true;
		}

		pendingCall.setConnectionForResult(0, bboxDBClient);
		pendingCall.setCompleteResult(0, false);
		pendingCall.setOperationResult(0, resultList);
		pendingCall.fireCompleteEvent();
		
		return true;
	}

}
