/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.network.server.handler.query;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.QueryBoundingBoxTimeRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.server.ClientConnectionHandler;
import org.bboxdb.network.server.ErrorMessages;
import org.bboxdb.network.server.StreamClientQuery;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.queryprocessor.queryplan.BoundingBoxAndTimeQueryPlan;
import org.bboxdb.storage.queryprocessor.queryplan.QueryPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandleBoundingBoxTimeQuery implements QueryHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(HandleBoundingBoxTimeQuery.class);
	

	@Override
	/**
	 * Handle the bounding box time query
	 */
	public void handleQuery(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) 
					throws IOException {
		
		try {
			if(clientConnectionHandler.getActiveQueries().containsKey(packageSequence)) {
				logger.error("Query sequence {} is allready known, please close old query first", packageSequence);
			}
			
			final QueryBoundingBoxTimeRequest queryRequest = QueryBoundingBoxTimeRequest.decodeTuple(encodedPackage);
			final TupleStoreName requestTable = queryRequest.getTable();
	
			final QueryPlan queryPlan = new BoundingBoxAndTimeQueryPlan(queryRequest.getBoundingBox(), 
					queryRequest.getTimestamp());
	
			final StreamClientQuery clientQuery = new StreamClientQuery(queryPlan, queryRequest.isPagingEnabled(), 
					queryRequest.getTuplesPerPage(), clientConnectionHandler, packageSequence, requestTable);
			
			clientConnectionHandler.getActiveQueries().put(packageSequence, clientQuery);
			clientConnectionHandler.sendNextResultsForQuery(packageSequence, packageSequence);
		} catch (PackageEncodeException e) {
			logger.warn("Got exception while decoding package", e);
			clientConnectionHandler.writeResultPackageNE(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
		}		
	}
}
