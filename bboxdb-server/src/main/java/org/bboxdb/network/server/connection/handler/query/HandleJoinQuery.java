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
package org.bboxdb.network.server.connection.handler.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.network.packets.PacketEncodeException;
import org.bboxdb.network.packets.request.QueryJoinRequest;
import org.bboxdb.network.packets.response.ErrorResponse;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.bboxdb.network.server.query.ErrorMessages;
import org.bboxdb.network.server.query.QueryHelper;
import org.bboxdb.network.server.query.StreamClientQuery;
import org.bboxdb.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.query.queryprocessor.OperatorTreeBuilder;
import org.bboxdb.query.queryprocessor.operator.Operator;
import org.bboxdb.query.queryprocessor.operator.SpatialIndexReadOperator;
import org.bboxdb.query.queryprocessor.operator.join.IndexedSpatialJoinOperator;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandleJoinQuery implements QueryHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(HandleJoinQuery.class);
	

	@Override
	/**
	 * Handle a bounding box query
	 */
	public void handleQuery(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) 
					throws IOException, PacketEncodeException {
		
		try {
			if(clientConnectionHandler.getActiveQueries().containsKey(packageSequence)) {
				logger.error("Query sequence {} is allready known, please close old query first", packageSequence);
				return;
			}
			
			final QueryJoinRequest queryRequest = QueryJoinRequest.decodeTuple(encodedPackage);
			final List<TupleStoreName> requestTables = queryRequest.getTables();
			final Hyperrectangle boundingBox = queryRequest.getBoundingBox();
			final List<UserDefinedFilterDefinition> udfs = queryRequest.getUdfs();
			
			for(final TupleStoreName requestTable : requestTables) {
				if(! QueryHelper.handleNonExstingTable(requestTable, packageSequence, clientConnectionHandler)) {
					return;
				}
			}
			
			final OperatorTreeBuilder operatorTreeBuilder = new OperatorTreeBuilder() {
				
				@Override
				public Operator buildOperatorTree(final List<TupleStoreManager> storageManager) {
					
					if(storageManager.size() <= 1) {
						throw new IllegalArgumentException("This operator tree needs more than one storage manager");
					}
					
					if(! udfs.isEmpty() && storageManager.size() != 2) {
						throw new IllegalArgumentException("Unable to use user defined filter on muti-join");
					}
					
					if(storageManager.size() == 2) {
						final Operator operator1 = new SpatialIndexReadOperator(storageManager.get(0), boundingBox);
						final SpatialIndexReadOperator indexReader = new SpatialIndexReadOperator(storageManager.get(1), boundingBox);
						return new IndexedSpatialJoinOperator(operator1, indexReader, udfs);			
					}
					
					Operator operator1 = new SpatialIndexReadOperator(storageManager.get(0), boundingBox);
					SpatialIndexReadOperator indexReader = new SpatialIndexReadOperator(storageManager.get(1), boundingBox);
					operator1 = new IndexedSpatialJoinOperator(operator1, indexReader);
					
					for(int i = 3; i < storageManager.size(); i++) {
						indexReader = new SpatialIndexReadOperator(storageManager.get(i), boundingBox);
						operator1 = new IndexedSpatialJoinOperator(operator1, indexReader);
					}
					
					return operator1;
				}
			};
					
			final StreamClientQuery clientQuery = new StreamClientQuery(operatorTreeBuilder, queryRequest.isPagingEnabled(), 
					queryRequest.getTuplesPerPage(), clientConnectionHandler, packageSequence, requestTables);
			
			clientConnectionHandler.getActiveQueries().put(packageSequence, clientQuery);
			clientConnectionHandler.sendNextResultsForQuery(packageSequence, packageSequence);
		} catch (PacketEncodeException e) {
			logger.warn("Got exception while decoding package", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
		}		
	}
}
