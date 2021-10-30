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
import java.util.Arrays;
import java.util.List;

import org.bboxdb.network.packets.PacketEncodeException;
import org.bboxdb.network.packets.request.QueryVersionTimeRequest;
import org.bboxdb.network.packets.response.ErrorResponse;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.bboxdb.network.server.query.ErrorMessages;
import org.bboxdb.network.server.query.QueryHelper;
import org.bboxdb.network.server.query.StreamClientQuery;
import org.bboxdb.query.queryprocessor.OperatorTreeBuilder;
import org.bboxdb.query.queryprocessor.operator.FullTablescanOperator;
import org.bboxdb.query.queryprocessor.operator.NewerAsVersionTimeSelectionOperator;
import org.bboxdb.query.queryprocessor.operator.Operator;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandleVersionTimeQuery implements QueryHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(HandleVersionTimeQuery.class);
	

	@Override
	/**
	 * Handle a time query
	 */
	public void handleQuery(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) 
					throws IOException, PacketEncodeException {
				
		try {
			if(clientConnectionHandler.getActiveQueries().containsKey(packageSequence)) {
				logger.error("Query sequence {} is allready known, please close old query first", packageSequence);
				return;
			}
			
			final QueryVersionTimeRequest queryRequest = QueryVersionTimeRequest.decodeTuple(encodedPackage);
			final TupleStoreName requestTable = queryRequest.getTable();
			
			if(! QueryHelper.handleNonExstingTable(requestTable, packageSequence, clientConnectionHandler)) {
				return;
			}
			
			final OperatorTreeBuilder operatorTreeBuilder = new OperatorTreeBuilder() {
				
				@Override
				public Operator buildOperatorTree(final List<TupleStoreManager> storageManager) {
					
					if(storageManager.size() != 1) {
						throw new IllegalArgumentException("This operator tree needs 1 storage manager");
					}
				
					final FullTablescanOperator tablescanOperator = new FullTablescanOperator(storageManager.get(0));
					final long timestamp = queryRequest.getTimestamp();
										
					final Operator opeator = new NewerAsVersionTimeSelectionOperator(timestamp, 
							tablescanOperator);
					
					return opeator;
				}
			};
			
			final StreamClientQuery clientQuery = new StreamClientQuery(operatorTreeBuilder, queryRequest.isPagingEnabled(), 
					queryRequest.getTuplesPerPage(), clientConnectionHandler, packageSequence, Arrays.asList(requestTable));
			
			clientConnectionHandler.getActiveQueries().put(packageSequence, clientQuery);
			clientConnectionHandler.sendNextResultsForQuery(packageSequence, packageSequence);
		} catch (PacketEncodeException e) {
			logger.warn("Got exception while decoding package", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
		}
		
		return;
	}
}
