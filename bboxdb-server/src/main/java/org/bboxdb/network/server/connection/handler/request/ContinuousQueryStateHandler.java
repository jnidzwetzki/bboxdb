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
package org.bboxdb.network.server.connection.handler.request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import org.bboxdb.network.entity.ContinuousQueryServerState;
import org.bboxdb.network.packets.PacketEncodeException;
import org.bboxdb.network.packets.request.ContinuousQueryStateRequest;
import org.bboxdb.network.packets.response.ContinuousQueryStateResponse;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.bboxdb.network.server.connection.ClientConnectionRegistry;
import org.bboxdb.network.server.query.ClientQuery;
import org.bboxdb.network.server.query.continuous.ContinuousClientQuery;
import org.bboxdb.network.server.query.continuous.ContinuousQueryExecutionState;
import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousQueryStateHandler implements RequestHandler {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ContinuousQueryStateHandler.class);
	
	@Override
	public boolean handleRequest(final ByteBuffer encodedPackage, final short packageSequence,
			final ClientConnectionHandler clientConnectionHandler) 
					throws IOException, PacketEncodeException {

		final ContinuousQueryServerState continuousQueryServerState = new ContinuousQueryServerState();
		
		final ContinuousQueryStateRequest request = ContinuousQueryStateRequest.decodeTuple(encodedPackage);
		final short sequenceNumber = request.getSequenceNumber();
		final TupleStoreName table = request.getTable();
		
		logger.debug("Got ContinuousQueryStateHandler package for table {}", table.getFullname());
		
		final ClientConnectionRegistry registry = clientConnectionHandler.getClientConnectionRegistry();
		final Set<ClientConnectionHandler> allConnections = registry.getAllActiveConnections();
		
		for(final ClientConnectionHandler connection : allConnections) {
			final Map<Short, ClientQuery> activeQueries = connection.getActiveQueries();
			
			for(final ClientQuery clientQuery : activeQueries.values()) {
				if(! (clientQuery instanceof ContinuousClientQuery)) {
					continue;
				}
				
				final ContinuousClientQuery continuousQuery = (ContinuousClientQuery) clientQuery;
				final String queryUUID = continuousQuery.getQueryPlan().getQueryUUID();
				
				final ContinuousQueryExecutionState state = continuousQuery.getContinuousQueryState();
				
				final Set<String> rangeKeys = state.getContainedTupleKeys();
				final Map<String, Set<String>> joinKeys = state.getContainedJoinedKeys();
				
				continuousQueryServerState.addRangeQueryState(queryUUID, rangeKeys);
				continuousQueryServerState.addJoinQueryState(queryUUID, joinKeys);
			}
		}
		
		final ContinuousQueryStateResponse response = new ContinuousQueryStateResponse(sequenceNumber, continuousQueryServerState);
		clientConnectionHandler.writeResultPackage(response);

		return true;
	}

}
