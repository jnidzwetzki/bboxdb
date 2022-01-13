/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.future.client.helper.ThreadedJoinedTupleListFutureIterator;
import org.bboxdb.network.client.future.network.NetworkOperationFuture;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.util.TimeBasedEntityDuplicateTracker;

public class JoinedTupleListFuture extends AbstractListFuture<MultiTuple>{

	public JoinedTupleListFuture(final Supplier<List<NetworkOperationFuture>> futures) {
		super(futures);
	}

	@Override
	protected Iterator<MultiTuple> createThreadedIterator() {
		return new ThreadedJoinedTupleListFutureIterator(this);
	}

	@Override
	protected Iterator<MultiTuple> createSimpleIterator() {
		final List<MultiTuple> allTuples = getListWithAllResults();
		final TimeBasedEntityDuplicateTracker entityDuplicateTracker = new TimeBasedEntityDuplicateTracker();

		final Iterator<MultiTuple> iterator = allTuples.iterator();
		while(iterator.hasNext()) {
			final MultiTuple nextElement = iterator.next();

			if(entityDuplicateTracker.isElementAlreadySeen(nextElement)) {
				iterator.remove();
			}
		}
		
		runShutdownCallbacks();
		
		return allTuples.iterator();
	}
	
	/**
	 * Get all used connections
	 * @return
	 */
	public Map<BBoxDBClient, List<Short>> getAllConnections() {
		
		final Map<BBoxDBClient, List<Short>> result = new HashMap<>();
		
		for(final NetworkOperationFuture future : futures) {
			final BBoxDBClient key = future.getConnection().getBboxDBClient();
			result.computeIfAbsent(key, k -> new ArrayList<Short>());
			result.get(key).add(future.getTransmittedPackage().getSequenceNumber());
		}
			
		return result;
	}

	/**
	 * Add further result tuples to process
	 * @param list
	 */
	public void addFuture(List<NetworkOperationFuture> list) {
		futures.addAll(list);		
	}
}
