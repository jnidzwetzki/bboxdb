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
package org.bboxdb.network.server.connection;

import java.util.Map;

import org.bboxdb.commons.concurrent.ExceptionSafeRunnable;
import org.bboxdb.commons.service.ServiceState;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.server.ClientQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionMaintenanceRunnable extends ExceptionSafeRunnable {

	/**
	 * The connection
	 */
	private final ClientConnectionHandler clientConnectionHandler;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ConnectionMaintenanceRunnable.class);


	public ConnectionMaintenanceRunnable(final ClientConnectionHandler clientConnectionHandler) {
		this.clientConnectionHandler = clientConnectionHandler;
	}

	@Override
	protected void beginHook() {
		logger.debug("Starting connection mainteinance thread for: {}", clientConnectionHandler.getConnectionName());
	}

	@Override
	protected void endHook() {
		logger.debug("Mainteinance thread for {} has terminated", clientConnectionHandler.getConnectionName());
	}

	@Override
	protected void runThread() throws Exception {
		final ServiceState serviceState = clientConnectionHandler.getConnectionState();

		while(serviceState.isInStartingState() || serviceState.isInRunningState()) {

			// Perform maintenance tasks for the queries
			final Map<Short, ClientQuery> queries = clientConnectionHandler.getActiveQueries();
			for(ClientQuery query : queries.values()) {
				query.maintenanceCallback();
			}
			
			// Write all waiting for compression packages
			clientConnectionHandler.flushPendingCompressionPackages();

			try {
				Thread.sleep(NetworkConst.MAX_COMPRESSION_DELAY_MS);
			} catch (InterruptedException e) {
				// Handle InterruptedException directly
				return;
			}
		}
	}
}

