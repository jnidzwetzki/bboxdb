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

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.future.JoinedTupleListFuture;
import org.bboxdb.network.client.future.OperationFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.PagedTransferableEntity;
import org.bboxdb.storage.entity.Tuple;

public class ResponseHandlerHelper {

	/**
	 * Cast the content on the result list according to the future type
	 * @param future
	 * @param resultList
	 * @param completeResult 
	 * @throws PackageEncodeException
	 */
	public static void castAndSetFutureResult(final OperationFuture future, 
			final List<PagedTransferableEntity> resultList, final boolean completeResult,
			final BBoxDBClient bboxDBClient) throws PackageEncodeException {
		
		if(future instanceof TupleListFuture) {
			final TupleListFuture pendingCall = (TupleListFuture) future;
			final List<Tuple> tupleList = new ArrayList<>();
			
			for(final PagedTransferableEntity entity : resultList) {
				tupleList.add((Tuple) entity);
			}
			
			pendingCall.setConnectionForResult(0, bboxDBClient);
			pendingCall.setCompleteResult(0, completeResult);	
			pendingCall.setOperationResult(0, tupleList);
			pendingCall.fireCompleteEvent();
		} else if(future instanceof JoinedTupleListFuture) {
			final JoinedTupleListFuture pendingCall = (JoinedTupleListFuture) future;
			final List<JoinedTuple> tupleList = new ArrayList<>();
			
			for(final PagedTransferableEntity entity : resultList) {
				tupleList.add((JoinedTuple) entity);
			}
			
			pendingCall.setConnectionForResult(0, bboxDBClient);
			pendingCall.setCompleteResult(0, completeResult);	
			pendingCall.setOperationResult(0, tupleList);
			pendingCall.fireCompleteEvent();
		} else {
			throw new PackageEncodeException("Unknown future type: " + future);
		}
	}
}
