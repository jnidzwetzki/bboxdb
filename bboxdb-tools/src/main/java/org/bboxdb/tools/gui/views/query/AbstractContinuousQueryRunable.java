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
package org.bboxdb.tools.gui.views.query;

import java.awt.Color;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.concurrent.ExceptionSafeRunnable;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.query.ContinuousQueryPlan;
import org.bboxdb.network.query.ContinuousSpatialJoinQueryPlan;
import org.bboxdb.storage.entity.EntityIdentifier;
import org.bboxdb.storage.entity.JoinedTupleIdentifier;
import org.bboxdb.storage.entity.JoinedTupleIdentifier.Strategy;
import org.bboxdb.storage.entity.MultiTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractContinuousQueryRunable extends ExceptionSafeRunnable {

	/**
	 * The query plan
	 */
	protected final ContinuousQueryPlan qp;
	
	/***
	 * The BBoxDB connection
	 */
	
	protected final BBoxDBCluster connection;
	
	/**
	 * The element overlay painter
	 */
	protected final ElementOverlayPainter painter;
	
	/**
	 * The last stale check
	 */
	private long lastStaleCheck = 0;
	
	/**
	 * Filter stale elements
	 */
	protected boolean FILTER_STALE_ELEMENTS = false;
	
	/**
	 * The update dates
	 */
	protected final Map<EntityIdentifier, Long> updateDates = new HashMap<>();
	
	/**
	 * The tuple versions
	 */
	protected final Map<EntityIdentifier, Long> tupleVersions = new HashMap<>();
	
	/**
	 * The painted elements
	 */
	protected final Map<EntityIdentifier, OverlayElementGroup> paintedElements = new HashMap<>();
	
	/**
	 * The duplicate handling strategy
	 */
	protected Strategy strategy;
	
	/**
	 * Stale time
	 */
	private long STALE_TIME_IN_MS = TimeUnit.MINUTES.toMillis(5);
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(AbstractContinuousQueryRunable.class);

	public AbstractContinuousQueryRunable(final ContinuousQueryPlan qp, 
			final BBoxDBCluster connection, final ElementOverlayPainter painter) {
		
		this.qp = qp;
		this.connection = connection;
		this.painter = painter;
		
		strategy = Strategy.KEY_AND_TABLE;
		
		if(qp instanceof ContinuousSpatialJoinQueryPlan) {
			final ContinuousSpatialJoinQueryPlan queryPlan = (ContinuousSpatialJoinQueryPlan) qp;
			
			if(! queryPlan.getAfterJoinFilter().isEmpty()) {
				strategy = Strategy.FIRST_KEY_AND_TABLE;
			}
			
			if( queryPlan.isReceiveInvalidations()) {
				strategy = Strategy.KEY_AND_TABLE;
			}
		}
		
		logger.info("Duplicate strategy is: {}", strategy);
	}

	/**
	 * Remove stale (old) tuple if needed from GUI
	 */
	protected void removeStaleTupleIfNeeded() {
		
		final long currentTime = System.currentTimeMillis();
		
		if(currentTime < lastStaleCheck + STALE_TIME_IN_MS) {
			return;
		}
		
		final Iterator<Entry<EntityIdentifier, Long>> iter = updateDates.entrySet().iterator();
		
		while (iter.hasNext()) {
			final Map.Entry<EntityIdentifier, Long> entry = (Map.Entry<EntityIdentifier, Long>) iter.next();
			
			if(entry.getValue() + STALE_TIME_IN_MS < currentTime) {
				iter.remove();
				tupleVersions.remove(entry.getKey());
				
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
	 * Remove the given tuple from the view
	 * @param joinedTuple
	 */
	protected void removeTupleFromView(final MultiTuple joinedTuple) {
		final EntityIdentifier key = new JoinedTupleIdentifier(joinedTuple, strategy);
		
		logger.info("---> Got invalidation tuple, removing {} from view", key);
				
		final OverlayElementGroup oldElement = paintedElements.remove(key);
		
		paintedElements.remove(key);
		tupleVersions.remove(key);
		updateDates.remove(key);
		
		if(oldElement != null) {
			painter.removeElementToDraw(oldElement);
		}
	}

	/**
	 * Update the given tuple on the GUI
	 * @param paintedElements
	 * @param updateDates 
	 * @param tupleVersions 
	 * @param joinedTuple
	 */
	protected void updateTupleOnGui(final MultiTuple joinedTuple, final List<Color> colors) {
		
		final OverlayElementGroup overlayElementGroup = OverlayElementBuilder.createOverlayElementGroup(
				joinedTuple, colors);
		
		final EntityIdentifier key = new JoinedTupleIdentifier(joinedTuple, strategy);
				
		final OverlayElementGroup oldElement = paintedElements.get(key);
	
		if(oldElement != null) {
			if(FILTER_STALE_ELEMENTS) {
				final long existingTimestamp = tupleVersions.get(key);
				
				if(joinedTuple.getVersionTimestamp() < existingTimestamp) {
					logger.info("Ignoring outdated version for tuple {} old={} new={}", key, 
							existingTimestamp, joinedTuple.getVersionTimestamp());
					return;
				}
			}
			
			painter.removeElementToDraw(oldElement);
		} 
		
		paintedElements.put(key, overlayElementGroup);
		tupleVersions.put(key, joinedTuple.getVersionTimestamp());
		updateDates.put(key, System.currentTimeMillis());
	
		painter.addElementToDraw(overlayElementGroup);
	}

}