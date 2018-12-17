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
package org.bboxdb.networkproxy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.networkproxy.handler.CloseHandler;
import org.bboxdb.networkproxy.handler.DeleteHandler;
import org.bboxdb.networkproxy.handler.GetHandler;
import org.bboxdb.networkproxy.handler.GetLocalDataHandler;
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
	private final BufferedReader socketReader;

	/**
	 * The socket writer
	 */
	private final Writer socketWriter;

	/**
	 * The bboxDB client
	 */
	private final BBoxDB bboxdbClient;

	/**
	 * The command handler
	 */
	private final static Map<String, ProxyCommandHandler> handler;

	static {
		handler = new HashMap<>();
		handler.put("PUT", new PutHandler());
		handler.put("GET", new GetHandler());
		handler.put("DELETE", new DeleteHandler());
		handler.put("GET_LOCAL_DATA", new GetLocalDataHandler());
		handler.put("RANGE_QUERY", new RangeQueryHandler());
		handler.put("CLOSE", new CloseHandler());
	}

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ProxyConnectionRunable.class);

	public ProxyConnectionRunable(final BBoxDB bboxdbClient, final Socket clientSocket) throws IOException {
		this.bboxdbClient = bboxdbClient;
		this.clientSocket = clientSocket;
		this.socketReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		this.socketWriter = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
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
		CloseableHelper.closeWithoutException(socketReader);
		CloseableHelper.closeWithoutException(socketWriter);
		CloseableHelper.closeWithoutException(clientSocket);
	}

	/**
	 * Read the next command from socket
	 * @throws IOException
	 */
	private void readNextCommand() throws IOException {
		final String commandLine = socketReader.readLine();
		final String command = commandLine.split(" ")[0];

		logger.info("Read command line {} / command {}", commandLine, command);

		final ProxyCommandHandler commandHandler = handler.get(command);

		if(commandHandler == null) {
			throw new IllegalArgumentException("Got unknown command: " + command);
		}

		commandHandler.handleCommand(bboxdbClient, commandLine, socketReader, socketWriter);

		// Flush written data
		socketWriter.flush();
	}

}
