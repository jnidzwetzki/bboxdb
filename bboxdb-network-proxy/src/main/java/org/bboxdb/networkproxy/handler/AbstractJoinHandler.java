/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
package org.bboxdb.networkproxy.handler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.JoinedTupleListFuture;
import org.bboxdb.networkproxy.ProxyConst;
import org.bboxdb.networkproxy.ProxyHelper;
import org.bboxdb.networkproxy.misc.TupleStringSerializer;
import org.bboxdb.storage.entity.JoinedTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJoinHandler implements ProxyCommandHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(AbstractJoinHandler.class);

	/**
	 * Perform the logging
	 * @param table
	 * @param bbox
	 */
	protected abstract void executeLogging(final String table1, final String table2, final Hyperrectangle bbox);
	
	/**
	 * Get the connection
	 * @param bboxdbClient 
	 * @return
	 */
	public abstract BBoxDB getConnection(final BBoxDBCluster bboxdbClient);
	
	@Override
	public void handleCommand(final BBoxDBCluster bboxdbClient, final InputStream socketInputStream,
			final OutputStream socketOutputStream) throws IOException {

		final String table1 = ProxyHelper.readStringFromServer(socketInputStream);
		final String table2 = ProxyHelper.readStringFromServer(socketInputStream);
		final String boundingBoxString = ProxyHelper.readStringFromServer(socketInputStream);

		final Hyperrectangle bbox = Hyperrectangle.fromString(boundingBoxString);

		executeLogging(table1, table2, bbox);
		
		try {
			final BBoxDB client = getConnection(bboxdbClient);
			final JoinedTupleListFuture tupleResult = client.queryJoin(Arrays.asList(table1, table2), bbox);
			tupleResult.waitForCompletion();

			for(final JoinedTuple tuple : tupleResult) {
				socketOutputStream.write(ProxyConst.RESULT_FOLLOW);
				TupleStringSerializer.writeJoinedTuple(tuple, socketOutputStream);
			}

			socketOutputStream.write(ProxyConst.RESULT_OK);
		} catch(InterruptedException e) {
			logger.debug("Got interrupted exception while handling bboxdb call");
			Thread.currentThread().interrupt();
			socketOutputStream.write(ProxyConst.RESULT_FAILED);
		} catch (Exception e) {
			logger.error("Got exception while proessing bboxdb call", e);
			socketOutputStream.write(ProxyConst.RESULT_FAILED);
		}
	}

}
