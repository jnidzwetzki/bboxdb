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
package org.bboxdb.networkproxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.network.client.future.client.TupleListFuture;
import org.bboxdb.networkproxy.misc.TupleStringSerializer;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.util.TupleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

public class ProxyHelper {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ProxyHelper.class);

	/**
	 * Read a string from a input stream
	 *
	 * @param inputStream
	 * @return
	 * @throws IOException
	 */
	public static String readStringFromServer(final InputStream inputStream) throws IOException {
		final int stringLength = DataEncoderHelper.readIntFromStream(inputStream);

		final byte[] stringBytes = new byte[stringLength];
		ByteStreams.readFully(inputStream, stringBytes, 0, stringBytes.length);

		return new String(stringBytes);
	}


	/**
	 * Write a tuple result to the client
	 * @param socketOutputStream
	 * @param tupleResult
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void writeTupleResult(final OutputStream socketOutputStream,
			final TupleListFuture tupleResult) throws InterruptedException, IOException {

		tupleResult.waitForCompletion();

		if(tupleResult.isFailed()) {
			logger.error("Got error while receiving tupeles: {}", tupleResult.getAllMessages());
			socketOutputStream.write(ProxyConst.RESULT_FAILED);
		} else {
			for(final Tuple tuple : tupleResult) {

				if(TupleHelper.isDeletedTuple(tuple)) {
					continue;
				}

				socketOutputStream.write(ProxyConst.RESULT_FOLLOW);
				TupleStringSerializer.writeTuple(tuple, socketOutputStream);
			}

			socketOutputStream.write(ProxyConst.RESULT_OK);
		}
	}

}
