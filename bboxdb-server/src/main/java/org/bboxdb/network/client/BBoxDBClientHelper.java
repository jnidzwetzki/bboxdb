/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.future.client.AbstractListFuture;
import org.bboxdb.network.client.future.client.EmptyResultFuture;
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
	public static void cancelQuery(final Map<BBoxDBClient, Short> cancelData)
			throws BBoxDBException, InterruptedException {
		
		for(Entry<BBoxDBClient, Short> entry : cancelData.entrySet()) {
			
			final BBoxDBClient client = entry.getKey();
			final short queryId = entry.getValue();

			final EmptyResultFuture cancelResult = client.cancelRequest(queryId);

			cancelResult.waitForCompletion();

			if(cancelResult.isFailed()) {
				throw new BBoxDBException("Cancel query has failed: " + cancelResult.getAllMessages());
			}
		}
	}
	
	/**
	 * Cancel a complete query
	 * @param future
	 * @throws InterruptedException
	 */
	public static void cancelQuery(final AbstractListFuture<? extends Object> future)
			throws BBoxDBException, InterruptedException {
		
		final Map<BBoxDBClient, Short> cancelData = new HashMap<>();

		for(int i = 0; i < future.getNumberOfResultObjets(); i++) {
			final short queryId = future.getRequestId(i);
			logger.info("Canceling query: {}", queryId);

			final BBoxDBConnection connection = future.getConnection(i);
			final BBoxDBClient client = connection.getBboxDBClient();
			
			cancelData.put(client, queryId);
		}
		
		cancelQuery(cancelData);
	}
}