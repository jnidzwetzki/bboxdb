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

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.RoutingHeaderHelper;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.util.EntityDuplicateTracker;
import org.bboxdb.storage.util.TupleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleListFuture extends AbstractListFuture<Tuple> {

	/**
	 * The duplicate resolver
	 */
	private final DuplicateResolver<Tuple> duplicateResolver;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleListFuture.class);

	/**
	 * The tablename for the read opeation
	 */
	private final String tablename;

	public TupleListFuture(final DuplicateResolver<Tuple> duplicateResolver, final String tablename) {
		this.duplicateResolver = duplicateResolver;
		this.tablename = tablename;
	}

	public TupleListFuture(final int numberOfFutures, final DuplicateResolver<Tuple> duplicateResolver, final String tablename) {
		super(numberOfFutures);
		
		this.duplicateResolver = duplicateResolver;
		this.tablename = tablename;
	}

	/**
	 * Create a new threaded iterator
	 * @return
	 */
	@Override
	protected ThreadedTupleListFutureIterator createThreadedIterator() {
		return new ThreadedTupleListFutureIterator(this);
	}
	
	/**
	 * Returns a simple iterator, used for non paged results
	 * @return
	 */
	@Override
	protected Iterator<Tuple> createSimpleIterator() {
		final List<Tuple> allTuples = getListWithAllResults();
		
		// Sort tuples
		allTuples.sort(TupleHelper.TUPLE_KEY_AND_VERSION_COMPARATOR);
		
		// Remove duplicates
		duplicateResolver.removeDuplicates(allTuples);
		
		// Perform read repair
		performReadRepair(allTuples);
		
		final EntityDuplicateTracker entityDuplicateTracker = new EntityDuplicateTracker();
		
		final Iterator<Tuple> iterator = allTuples.iterator();
		while(iterator.hasNext()) {
			final Tuple nextElement = iterator.next();
			
			if(entityDuplicateTracker.isElementAlreadySeen(nextElement)) {
				iterator.remove();
			}
		}
		
		return allTuples.iterator();
	}

	/**
	 * Perform read repair by checking all results
	 */
	private void performReadRepair(final List<Tuple> allTuples) {
		
		// Unable to perform read repair on only one result object
		if(getNumberOfResultObjets() < 2) {
			return;
		}
		
		try {
			for(int resultId = 0; resultId < getNumberOfResultObjets(); resultId++) {
				performReadRepairForResult(allTuples, resultId);	
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		} catch (BBoxDBException | ZookeeperException e) {
			logger.error("Got exception during read repair", e);
		}
	}

	/**
	 * Perform read repair for the given result
	 * 
	 * @param allTuples
	 * @param resultId
	 * @throws InterruptedException
	 * @throws BBoxDBException
	 * @throws ZookeeperException 
	 */
	private void performReadRepairForResult(final List<Tuple> allTuples, int resultId)
			throws InterruptedException, BBoxDBException, ZookeeperException {
		
		final List<Tuple> tupleResult = get(resultId);

		final BBoxDBClient bboxDBConnection = getConnection(resultId);
		
		if(bboxDBConnection == null) {
			// Unable to perform read repair when the connection is not known
			return;
		}
		
		for(final Tuple tuple : allTuples) {
			final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystem(
					tablename, tuple.getBoundingBox(), true, bboxDBConnection.getServerAddress(), true);
					
			// System is not responsible for the tuple
			if(routingHeader.getHopCount() == 0) {
				return;
			}
			
			if(! tupleResult.contains(tuple)) {
				
				logger.info("Tuple {} is not contained in result {} from server {}, "
						+ "performing read repair", tuple, tupleResult, 
						bboxDBConnection.getConnectionName());
				
				final Supplier<RoutingHeader> routingHeaderSupplier = () -> (routingHeader);
				bboxDBConnection.insertTuple(tablename, tuple, routingHeaderSupplier);
			}
		}
	}	
}
