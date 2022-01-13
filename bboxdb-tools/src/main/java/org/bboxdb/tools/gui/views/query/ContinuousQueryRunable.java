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
package org.bboxdb.tools.gui.views.query;

import java.awt.Color;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.ContinuousQueryState;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.query.ContinuousQueryPlan;
import org.bboxdb.storage.entity.IdleQueryStateRemovedTuple;
import org.bboxdb.storage.entity.InvalidationTuple;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.entity.WatermarkTuple;
import org.bboxdb.storage.sstable.SSTableConst;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousQueryRunable extends AbstractContinuousQueryRunable {
	
	/**
	 * The color of the results
	 */
	private final List<Color> colors;
	
	/**
	 * The query result
	 */
	private JoinedTupleListFuture queryResult;
	
	/**
	 * The seen watermarks
	 */
	private final Set<Long> seenWatermarks;
	
	/**
	 * The waiting queue if watermarks are active
	 */
	private final TransferQueue<MultiTuple> watermarkQueue;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ContinuousQueryRunable.class);

	public ContinuousQueryRunable(final List<Color> colors, final ContinuousQueryPlan qp, 
			final BBoxDBCluster connection, final ElementOverlayPainter painter) {
		
		super(qp, connection, painter);
		this.colors = colors;
		this.seenWatermarks = new HashSet<>();
		this.watermarkQueue = new LinkedTransferQueue<>();
	}
	
	@Override
	protected void runThread() throws Exception {
	
		logger.info("New worker thread for a continuous query has started");
		queryResult = connection.queryContinuous(qp);
	
		try {
			queryResult.waitForCompletion();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	
		for(final MultiTuple joinedTuple : queryResult) {
			if(queryResult.isFailed()) {
				logger.error("Got an error" + queryResult.getAllMessages());
				return;
			}
	
			if(Thread.currentThread().isInterrupted()) {
				return;
			}
			
			final Tuple firstTuple = joinedTuple.getTuple(0);
			
			if(firstTuple instanceof WatermarkTuple) {
				handleWatermark(firstTuple);
			} else {
				// Update stale tuples only if we don't receive watermarks
				// otherwise, the stale tuples are removed when all watermarks are present
				if(! qp.isReceiveWatermarks()) {
					
					if (firstTuple instanceof InvalidationTuple || firstTuple instanceof IdleQueryStateRemovedTuple) {
						removeTupleFromView(joinedTuple);
					} else {
						updateTupleOnGui(joinedTuple, colors, true);
						removeStaleTupleIfNeeded();
					}
					
				} else {
					// When watermarks active, queue elements
					final boolean queueResult = watermarkQueue.offer(joinedTuple);
					
					if(! queueResult) {
						logger.warn("Unable to queue tuple {}", joinedTuple);
					}
				}
			}
		}
	}
	
	/**
	 * Handle the watermark tuple
	 * @param firstTuple
	 */
	private void handleWatermark(final Tuple tuple) {
		
		logger.info("Handle watermark {}", tuple);
		
		final String tupleStoreString = tuple.getKey().replace(SSTableConst.WATERMARK_KEY + "_", "");
		final TupleStoreName tupleStore = new TupleStoreName(tupleStoreString);
		final long regionId = tupleStore.getRegionId().getAsLong();
		seenWatermarks.add(regionId);
		
		final String queryUUID = qp.getQueryUUID();
		final Optional<ContinuousQueryState> queryStateOptional = connection.getContinousQueryState(queryUUID);

		if(! queryStateOptional.isPresent()) {
			logger.error("Query state is not present, unable to handle watermark");
			return;
		}
		
		final ContinuousQueryState queryState = queryStateOptional.get();
		
		// All watermarks are present
		if(seenWatermarks.size() == queryState.getRegisteredRegions().size()) {
			handleWatermarkDone();
		}
	}

	/**
	 * Handle the completion of the watermark
	 */
	private void handleWatermarkDone() {
		logger.debug("Watermark complete: {}", seenWatermarks);
		
		// Paint pending elements
		final List<MultiTuple> queue = new LinkedList<>();
		watermarkQueue.drainTo(queue);
		logger.info("Processing {} waiting elements", queue.size());
		
		for(final MultiTuple multiTuple : queue) {
			
			final Tuple firstTuple = multiTuple.getTuple(0);

			if (firstTuple instanceof InvalidationTuple) {
				removeTupleFromView(multiTuple);
			} else {
				updateTupleOnGui(multiTuple, colors, false);
			}
		}
		
		seenWatermarks.clear();
		removeStaleTupleIfNeeded();
		painter.repaintAll();
	}

	@Override
	protected void endHook() { 
		
		if(queryResult != null) {
			logger.info("Canceling continuous query");
			
			// Clear interrupted flag, to be able to cancel the query on server
			boolean interruptedFlag = Thread.interrupted();
			
			try {
				logger.info("Canceling continous query {}", qp.getQueryUUID());
				connection.cancelContinousQuery(qp.getQueryUUID());
			} catch (BBoxDBException e) {
				logger.error("Got exception", e);
			} catch (InterruptedException e) {
				logger.info("Got interrupted exception");
				interruptedFlag = true;
			} finally {
				// Restore interrupted flag
				if(interruptedFlag) {
					Thread.currentThread().interrupt();
				}
			}
		}
		
		logger.info("Worker for continuous query exited");
	}
}
