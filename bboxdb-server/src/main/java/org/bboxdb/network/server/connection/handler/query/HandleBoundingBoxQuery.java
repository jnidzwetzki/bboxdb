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
package org.bboxdb.network.server.connection.handler.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.QueryHyperrectangleRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.query.filter.UserDefinedFilter;
import org.bboxdb.network.server.ErrorMessages;
import org.bboxdb.network.server.QueryHelper;
import org.bboxdb.network.server.StreamClientQuery;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.queryprocessor.OperatorTreeBuilder;
import org.bboxdb.storage.queryprocessor.operator.Operator;
import org.bboxdb.storage.queryprocessor.operator.SpatialIndexReadOperator;
import org.bboxdb.storage.queryprocessor.operator.UserDefinedFilterOperator;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandleBoundingBoxQuery implements QueryHandler {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(HandleBoundingBoxQuery.class);


	@Override
	/**
	 * Handle a bounding box query
	 */
	public void handleQuery(final ByteBuffer encodedPackage,
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler)
					throws IOException, PackageEncodeException {

		try {
			if(clientConnectionHandler.getActiveQueries().containsKey(packageSequence)) {
				logger.error("Query sequence {} is already known, please close old query first", packageSequence);
				return;
			}

			final QueryHyperrectangleRequest queryRequest = QueryHyperrectangleRequest.decodeTuple(encodedPackage);
			final TupleStoreName requestTable = queryRequest.getTable();

			if(! QueryHelper.handleNonExstingTable(requestTable, packageSequence, clientConnectionHandler)) {
				return;
			}

			final String userdefinedFilterName = queryRequest.getUserDefinedFilterName();
			final byte[] userDefinedFilterValue = queryRequest.getUserDefinedFilterValue();

			final OperatorTreeBuilder operatorTreeBuilder = new OperatorTreeBuilder() {

				@Override
				public Operator buildOperatorTree(final List<TupleStoreManager> storageManager) {

					if(storageManager.size() != 1) {
						throw new IllegalArgumentException("This operator tree needs 1 storage manager");
					}

					final Hyperrectangle boundingBox = queryRequest.getBoundingBox();
					final Operator indexReadOperator = new SpatialIndexReadOperator(
							storageManager.get(0), boundingBox);

					// Add the user defined filter operator
					if(! userdefinedFilterName.equals("")) {
						try {
							final Class<?> customFilterClass = Class.forName(userdefinedFilterName);
							final UserDefinedFilter userDefinedFilter
								= (UserDefinedFilter) customFilterClass.newInstance();

							return new UserDefinedFilterOperator(userDefinedFilter,
									userDefinedFilterValue, indexReadOperator);
						} catch (Exception e) {
							throw new IllegalArgumentException("Unable to load user defined filter", e);
						}
					} else {
						return indexReadOperator;
					}
				}
			};

			final StreamClientQuery clientQuery = new StreamClientQuery(operatorTreeBuilder, queryRequest.isPagingEnabled(),
					queryRequest.getTuplesPerPage(), clientConnectionHandler, packageSequence, Arrays.asList(requestTable));

			clientConnectionHandler.getActiveQueries().put(packageSequence, clientQuery);
			clientConnectionHandler.sendNextResultsForQuery(packageSequence, packageSequence);
		} catch (PackageEncodeException e) {
			logger.warn("Got exception while decoding package", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));
		}
	}
}
