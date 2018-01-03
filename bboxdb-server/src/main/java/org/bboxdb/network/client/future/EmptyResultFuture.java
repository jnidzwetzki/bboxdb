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
package org.bboxdb.network.client.future;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EmptyResultFuture extends OperationFutureImpl<Boolean> {

	public EmptyResultFuture() {
		super();
	}

	public EmptyResultFuture(final int numberOfFutures) {
		super(numberOfFutures);
	}
	
	
	@Override
	public Boolean get(int resultId) throws InterruptedException {
		
		// Wait for the future
		futures.get(resultId).get();
		
		// Return true, when the operation was succesfully
		return ! isFailed();
	}
	
	@Override
	public Boolean get(final int resultId, final long timeout, final TimeUnit unit)
			throws InterruptedException, TimeoutException {

		// Wait for the future
		futures.get(resultId).get(timeout, unit);

		// Return true, when the operation was succesfully
		return ! isFailed();
	}

}
