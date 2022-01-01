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
package org.bboxdb.networkproxy.handler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.client.EmptyResultFuture;
import org.bboxdb.networkproxy.ProxyConst;
import org.bboxdb.networkproxy.ProxyHelper;
import org.bboxdb.networkproxy.misc.TupleStringSerializer;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutHandler implements ProxyCommandHandler {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(PutHandler.class);

	@Override
	public void handleCommand(final BBoxDBCluster bboxdbClient, final InputStream socketInputStream,
			final OutputStream socketOutputStream) throws IOException {

		final String table = ProxyHelper.readStringFromServer(socketInputStream);
		final Tuple tuple = TupleStringSerializer.readTuple(socketInputStream);

		logger.info("Got put call for table {} and tuple {} (is full box {})",
				table, tuple, tuple.getBoundingBox() == Hyperrectangle.FULL_SPACE);

		try {
			final EmptyResultFuture insertResult = bboxdbClient.put(table, tuple);
			insertResult.waitForCompletion();

			if(insertResult.isFailed()) {
				logger.error("--> Got failed put result: {}", insertResult.getAllMessages());
				socketOutputStream.write(ProxyConst.RESULT_FAILED);
			} else {
				socketOutputStream.write(ProxyConst.RESULT_OK);
			}
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
