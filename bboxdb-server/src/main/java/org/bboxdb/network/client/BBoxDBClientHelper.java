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
package org.bboxdb.network.client;

import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.future.AbstractListFuture;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BBoxDBClientHelper {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BBoxDBClientHelper.class);

	/**
	 * Cancel a complete query
	 * @param future
	 * @throws InterruptedException
	 */
	public static void cancelQuery(final AbstractListFuture<? extends Object> future)
			throws BBoxDBException, InterruptedException {

		for(int i = 0; i < future.getNumberOfResultObjets(); i++) {
			final short queryId = future.getRequestId(i);
			logger.info("Canceling query: {}", queryId);

			final BBoxDBConnection connection = future.getConnection(i);
			final BBoxDBClient client = connection.getBboxDBClient();

			final EmptyResultFuture cancelResult = client.cancelRequest(queryId);

			cancelResult.waitForCompletion();

			if(cancelResult.isFailed()) {
				throw new BBoxDBException("Cancel query has failed: " + cancelResult.getAllMessages());
			}
		}
	}

}
