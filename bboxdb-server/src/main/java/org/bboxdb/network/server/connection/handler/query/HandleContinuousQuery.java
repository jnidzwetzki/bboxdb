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
package org.bboxdb.network.server.connection.handler.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.QueryContinuousRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.query.ContinuousQueryPlan;
import org.bboxdb.network.server.ClientQuery;
import org.bboxdb.network.server.ContinuousClientQuery;
import org.bboxdb.network.server.ErrorMessages;
import org.bboxdb.network.server.QueryHelper;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandleContinuousQuery implements QueryHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(HandleContinuousQuery.class);
	

	@Override
	/**
	 * Handle a bounding box query
	 */
	public void handleQuery(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) 
					throws IOException, PackageEncodeException {
		
		try {
			final Map<Short, ClientQuery> activeQueries = clientConnectionHandler.getActiveQueries();
			
			if(activeQueries.containsKey(packageSequence)) {
				logger.error("Query sequence {} is already known, please close old query first", packageSequence);
				return;
			}
			
			final QueryContinuousRequest queryRequest = QueryContinuousRequest.decodeTuple(encodedPackage);
			
			final ContinuousQueryPlan queryPlan = queryRequest.getQueryPlan();
			final String requestTableString = queryPlan.getStreamTable();
			final TupleStoreName requestTable = new TupleStoreName(requestTableString);
			
			if(! QueryHelper.handleNonExstingTable(requestTable, packageSequence, clientConnectionHandler)) {
				return;
			}
						
			final ContinuousClientQuery clientQuery = new ContinuousClientQuery(queryPlan,
					clientConnectionHandler, packageSequence);
			
			final String newUUID = clientQuery.getQueryPlan().getQueryUUID();

			final boolean alreadyRegistered = isQueryAlreadyRegistered(packageSequence, clientConnectionHandler, activeQueries, newUUID);
		
			if(alreadyRegistered) {
				logger.error("Unable to register query, UUID {} already known", newUUID);
				clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_QUERY_CONTINOUS_DUPLICATE));	
			} else {
				activeQueries.put(packageSequence, clientQuery);
				clientConnectionHandler.sendNextResultsForQuery(packageSequence, packageSequence);
			}
		} catch (PackageEncodeException e) {
			logger.warn("Got exception while decoding package", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
		}		
	}


	/**
	 * Is the query with the given UUID already registered?
	 * @param packageSequence
	 * @param clientConnectionHandler
	 * @param activeQueries
	 * @param newUUID
	 * @return 
	 * @throws IOException
	 * @throws PackageEncodeException
	 */
	private boolean isQueryAlreadyRegistered(final short packageSequence,
			final ClientConnectionHandler clientConnectionHandler, final Map<Short, ClientQuery> activeQueries,
			final String newUUID) throws IOException, PackageEncodeException {
		
		for(final ClientQuery query : activeQueries.values()) {
			if(query instanceof ContinuousClientQuery) {
				final ContinuousClientQuery registeredQuery = (ContinuousClientQuery) query;
				final String knownUUID = registeredQuery.getQueryPlan().getQueryUUID();

				if(knownUUID.equals(newUUID)) {
					return true;
				}
			}
		}
		
		return false;
	}
}
