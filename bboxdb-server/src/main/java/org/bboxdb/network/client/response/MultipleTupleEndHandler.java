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
import java.util.List;
import java.util.Map;

import org.bboxdb.network.client.BBoxDBConnection;
import org.bboxdb.network.client.future.NetworkOperationFuture;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.MultipleTupleEndResponse;
import org.bboxdb.storage.entity.PagedTransferableEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultipleTupleEndHandler implements ServerResponseHandler {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(MultipleTupleEndHandler.class);

	@Override
	public boolean handleServerResult(final BBoxDBConnection bBoxDBConnection, 
			final ByteBuffer encodedPackage, final NetworkOperationFuture future)
			throws PackageEncodeException {
				
		if(logger.isDebugEnabled()) {
			logger.debug("Handle multiple tuple end package");
		}
				
		final MultipleTupleEndResponse result = MultipleTupleEndResponse.decodePackage(encodedPackage);
		
		final short sequenceNumber = result.getSequenceNumber();
		final Map<Short, List<PagedTransferableEntity>> resultBuffer = bBoxDBConnection.getResultBuffer();
		final List<PagedTransferableEntity> resultList = resultBuffer.remove(sequenceNumber);
		
		if(future == null) {
			logger.error("Got handleMultiTupleEnd and future is null (package {}) ", sequenceNumber);
			return true;
		}
		
		if(resultList == null) {
			logger.error("Got handleMultiTupleEnd and resultList is null (package {})", sequenceNumber);
			future.setFailedState();
			future.fireCompleteEvent();
			return true;
		}
		
		ResponseHandlerHelper.castAndSetFutureResult(future, resultList, true);

		return true;
	}
}
