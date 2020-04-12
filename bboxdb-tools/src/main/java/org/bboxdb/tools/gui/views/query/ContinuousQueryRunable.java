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
import java.util.List;

import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.network.query.ContinuousQueryPlan;
import org.bboxdb.storage.entity.JoinedTuple;
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
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ContinuousQueryRunable.class);

	public ContinuousQueryRunable(final List<Color> colors, final ContinuousQueryPlan qp, 
			final BBoxDB connection, final ElementOverlayPainter painter) {
		
		super(qp, connection, painter);
		this.colors = colors;
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
	
		for(final JoinedTuple joinedTuple : queryResult) {
			if(queryResult.isFailed()) {
				logger.error("Got an error" + queryResult.getAllMessages());
				return;
			}
	
			if(Thread.currentThread().isInterrupted()) {
				return;
			}
					
			updateTupleOnGui(joinedTuple, colors);
			removeStaleTupleIfNeeded();
		}
	}
	
	@Override
	protected void endHook() { 
		
		if(queryResult != null) {
			logger.info("Canceling continuous query");
			try {
				connection.cancelQuery(queryResult.getAllConnections());
			} catch (BBoxDBException e) {
				logger.error("Got exception", e);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		
		logger.info("Worker for continuous query exited");
	}
}
