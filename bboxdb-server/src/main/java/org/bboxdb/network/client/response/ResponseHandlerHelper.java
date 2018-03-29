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

import org.bboxdb.network.client.future.NetworkOperationFuture;
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
	public static void castAndSetFutureResult(final NetworkOperationFuture future, 
			final List<PagedTransferableEntity> resultList, final boolean completeResult) 
					throws PackageEncodeException {
		
		if(resultList.isEmpty()) {
			future.setCompleteResult(completeResult);
			future.setOperationResult(new ArrayList<>());
			future.fireCompleteEvent();
			return;
		}
		
		final PagedTransferableEntity firstElement = resultList.get(0);
		
		if(firstElement instanceof Tuple) {
			final List<Tuple> tupleList = new ArrayList<>();
			
			for(final PagedTransferableEntity entity : resultList) {
				tupleList.add((Tuple) entity);
			}
						
			future.setCompleteResult(completeResult);
			future.setOperationResult(tupleList);
			future.fireCompleteEvent();
		} else if(firstElement instanceof JoinedTuple) {
			final List<JoinedTuple> tupleList = new ArrayList<>();
			
			for(final PagedTransferableEntity entity : resultList) {
				tupleList.add((JoinedTuple) entity);
			}
			
			future.setCompleteResult(completeResult);
			future.setOperationResult(tupleList);
			future.fireCompleteEvent();
		} else {
			throw new PackageEncodeException("Unknown future type: " + firstElement);
		}
	}
}
