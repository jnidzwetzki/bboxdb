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
package org.bboxdb.networkproxy;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.networkproxy.handler.CloseHandler;
import org.bboxdb.networkproxy.handler.DeleteHandler;
import org.bboxdb.networkproxy.handler.GetHandler;
import org.bboxdb.networkproxy.handler.JoinHandler;
import org.bboxdb.networkproxy.handler.JoinLocalHandler;
import org.bboxdb.networkproxy.handler.RangeQueryLocalHandler;
import org.bboxdb.networkproxy.handler.ProxyCommandHandler;
import org.bboxdb.networkproxy.handler.PutHandler;
import org.bboxdb.networkproxy.handler.RangeQueryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyConnectionRunable implements Runnable {

	/**
	 * The client socket
	 */
	private final Socket clientSocket;

	/**
	 * The socket reader
	 */
	private final InputStream socketInputStream;

	/**
	 * The socket writer
	 */
	private final OutputStream socketOutputStream;

	/**
	 * The bboxDB client
	 */
	private final BBoxDBCluster bboxdbClient;

	/**
	 * The command handler
	 */
	private final static Map<Byte, ProxyCommandHandler> handler;

	static {
		handler = new HashMap<>();
		handler.put(ProxyConst.COMMAND_PUT, new PutHandler());
		handler.put(ProxyConst.COMMAND_GET, new GetHandler());
		handler.put(ProxyConst.COMMAND_DELETE, new DeleteHandler());
		handler.put(ProxyConst.COMMAND_RANGE_QUERY_LOCAL, new RangeQueryLocalHandler());
		handler.put(ProxyConst.COMMAND_RANGE_QUERY, new RangeQueryHandler());
		handler.put(ProxyConst.COMMAND_CLOSE, new CloseHandler());
		handler.put(ProxyConst.COMMAND_JOIN, new JoinHandler());
		handler.put(ProxyConst.COMMAND_JOIN_LOCAL, new JoinLocalHandler());
	}

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ProxyConnectionRunable.class);

	public ProxyConnectionRunable(final BBoxDBCluster bboxdbClient, final Socket clientSocket) throws IOException {
		this.bboxdbClient = bboxdbClient;
		this.clientSocket = clientSocket;
		this.socketInputStream = new BufferedInputStream(clientSocket.getInputStream());
		this.socketOutputStream = new BufferedOutputStream(clientSocket.getOutputStream());
	}

	@Override
	public void run() {
		try {
			while(! Thread.currentThread().isInterrupted()) {
				readNextCommand();
			}
		} catch(Throwable e) {
			logger.error("Got exception while processing commands", e);
		} finally {
			closeConnection();
		}
	}

	/**
	 * Close the connection
	 */
	private void closeConnection() {
		logger.info("Closing connection to: {}", clientSocket.getRemoteSocketAddress());
		CloseableHelper.closeWithoutException(socketInputStream);
		CloseableHelper.closeWithoutException(socketOutputStream);
		CloseableHelper.closeWithoutException(clientSocket);
	}

	/**
	 * Read the next command from socket
	 * @throws IOException
	 */
	private void readNextCommand() throws IOException {
		final byte command = (byte) socketInputStream.read();

		logger.info("Read command {}", command);

		final ProxyCommandHandler commandHandler = handler.get(command);

		if(commandHandler == null) {
			throw new IllegalArgumentException("Got unknown command: " + command);
		}

		commandHandler.handleCommand(bboxdbClient, socketInputStream, socketOutputStream);

		// Flush written data
		socketOutputStream.flush();
	}

}
