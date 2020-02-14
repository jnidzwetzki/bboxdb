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
package org.bboxdb.tools.gui.views.query;

import java.awt.Color;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.concurrent.ExceptionSafeRunnable;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.network.query.ContinuousQueryPlan;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousQueryRunable extends ExceptionSafeRunnable {
	
	/**
	 * The name of the table to query
	 */
	private final String table;
	
	/**
	 * The color of the results
	 */
	private final Color color;
	
	/**
	 * The query plan
	 */
	private final ContinuousQueryPlan qp;

	/***
	 * The BBoxDB connection
	 */
	private final BBoxDB connection;

	/**
	 * The element overlay painter
	 */
	private final ElementOverlayPainter painter;
	
	/**
	 * The last stale check
	 */
	private long lastStaleCheck = 0;
	
	/**
	 * Stale time
	 */
	private long STALE_TIME_IN_MS = TimeUnit.MINUTES.toMillis(5);

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ContinuousQueryRunable.class);


	public ContinuousQueryRunable(final String table, final Color color, final ContinuousQueryPlan qp, 
			final BBoxDB connection, final ElementOverlayPainter painter) {
		
		this.table = table;
		this.color = color;
		this.qp = qp;
		this.connection = connection;
		this.painter = painter;
	}

	@Override
	protected void runThread() throws Exception {

		logger.info("New worker thread for a continuous query has started");
		final JoinedTupleListFuture result = connection.queryContinuous(qp);

		try {
			result.waitForCompletion();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}

		final Map<String, Long> updateDates = new HashMap<>();
		final Map<String, OverlayElementGroup> paintedElements = new HashMap<>();

		for(final JoinedTuple joinedTuple : result) {
			if(result.isFailed()) {
				logger.error("Got an error" + result.getAllMessages());
				return;
			}

			if(Thread.currentThread().isInterrupted()) {
				return;
			}

			logger.debug("Read a tuple from: {}", table);
			
			updateTupleOnGui(paintedElements, updateDates, joinedTuple);
			removeStaleTupleIfNeeded(paintedElements, updateDates);
		}
	}
	
	@Override
	protected void endHook() { 
		logger.info("Worker for continuous query exited");
	}

	/**
	 * Remove stale (old) tuple if needed from GUI
	 * @param updateDates 
	 * @param paintedElements 
	 */
	private void removeStaleTupleIfNeeded(final Map<String, OverlayElementGroup> paintedElements, 
			final Map<String, Long> updateDates) {
		
		final long currentTime = System.currentTimeMillis();
		
		if(currentTime < lastStaleCheck + STALE_TIME_IN_MS) {
			return;
		}
		
		final Iterator<Entry<String, Long>> iter = updateDates.entrySet().iterator();
		
		while (iter.hasNext()) {
			final Map.Entry<String, Long> entry = (Map.Entry<String, Long>) iter.next();
			
			if(entry.getValue() + STALE_TIME_IN_MS < currentTime) {
				iter.remove();
				
				final OverlayElementGroup oldElement = paintedElements.remove(entry.getKey());
				
				if(oldElement != null) {
					logger.info("Removed one stale element");
					painter.removeElementToDraw(oldElement);
				}
			}
		}
		
		lastStaleCheck = currentTime;
	}

	/**
	 * Update the given tuple on the GUI
	 * @param paintedElements
	 * @param updateDates 
	 * @param joinedTuple
	 */
	private void updateTupleOnGui(final Map<String, OverlayElementGroup> paintedElements, 
			final Map<String, Long> updateDates, final JoinedTuple joinedTuple) {
		
		final Tuple tuple = joinedTuple.getTuple(0);
		final OverlayElement overlayElement = OverlayElementHelper.getOverlayElement(tuple, table, color);
		final OverlayElementGroup newElement = new OverlayElementGroup(Arrays.asList(overlayElement));

		final String key = tuple.getKey();
		
		final OverlayElementGroup oldElement = paintedElements.get(key);

		if(oldElement != null) {
			painter.removeElementToDraw(oldElement);
		}

		paintedElements.put(key, newElement);
		updateDates.put(key, System.currentTimeMillis());
		painter.addElementToDraw(newElement);
	}
}
