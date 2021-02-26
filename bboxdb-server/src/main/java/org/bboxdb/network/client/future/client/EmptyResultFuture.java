/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.network.client.future.client;

import java.util.List;
import java.util.function.Supplier;

import org.bboxdb.network.client.future.network.NetworkOperationFuture;

public class EmptyResultFuture extends OperationFutureImpl<Boolean> {

	public EmptyResultFuture(final Supplier<List<NetworkOperationFuture>> futures) {
		super(futures);
	}

	public EmptyResultFuture(final Supplier<List<NetworkOperationFuture>> futures,
			final FutureRetryPolicy retryPolicy) {
		super(futures, retryPolicy);
	}

	@Override
	public Boolean get(int resultId) throws InterruptedException {

		// Wait for the future
		futures.get(resultId).get(true);

		// Return true, when the operation was successfully
		return ! isFailed();
	}

}
