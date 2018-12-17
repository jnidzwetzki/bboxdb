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
package org.bboxdb.networkproxy.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.networkproxy.ProxyConst;
import org.bboxdb.storage.entity.Tuple;

public class NetworkProxyClient implements AutoCloseable {

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

	public NetworkProxyClient(final String hostname, final int port)
			throws UnknownHostException, IOException {

		this.clientSocket = new Socket(hostname, port);
		this.socketInputStream = new BufferedInputStream(clientSocket.getInputStream());
		this.socketOutputStream = new BufferedOutputStream(clientSocket.getOutputStream());
	}

	@Override
	public void close() throws IOException {
		CloseableHelper.closeWithoutException(socketInputStream);
		CloseableHelper.closeWithoutException(socketOutputStream);
		CloseableHelper.closeWithoutException(clientSocket);
	}

	/**
	 * Send data to server
	 * @throws IOException
	 */
	private synchronized void sendToServer(final byte byteData)
			throws IOException {

		socketOutputStream.write(byteData);
		socketOutputStream.flush();
	}

	/**
	 * Send data to server
	 * @throws IOException
	 */
	private synchronized void sendToServer(final String string)
			throws IOException {

		final int stringLength = string.length();
		socketOutputStream.write(DataEncoderHelper.intToByteBuffer(stringLength).array());
		socketOutputStream.write(string.getBytes());
		socketOutputStream.flush();
	}

	/**
	 * Wait for the server result
	 * @throws IOException
	 */
	public synchronized void waitForServerOkResult() throws IOException {
		final byte serverResult = (byte) socketInputStream.read();

		if(ProxyConst.RESULT_OK != serverResult) {
			throw new IOException("Read " + serverResult
					+ " from server instead of: " + ProxyConst.RESULT_OK);
		}
	}

	/**
	 * The get call
	 * @param key
	 * @param table
	 * @return
	 * @throws IOException
	 */
	public synchronized List<Tuple> get(final String key, final String table) throws IOException {
		sendToServer(ProxyConst.COMMAND_GET);
		sendToServer(table);
		sendToServer(key);

		return new ArrayList<>();
	}

	/**
	 * The get local call
	 * @param key
	 * @param table
	 * @return
	 * @throws IOException
	 */
	public synchronized List<Tuple> getLocal(final String key, final String table) throws IOException {
		sendToServer(ProxyConst.COMMAND_GET_LOCAL);
		sendToServer(table);
		sendToServer(key);

		return new ArrayList<>();
	}

	/**
	 * The range query call
	 * @param queryRectangle
	 * @param table
	 * @return
	 * @throws IOException
	 */
	public synchronized List<Tuple> rangeQuery(final Hyperrectangle queryRectangle, final String table)
			throws IOException {

		sendToServer(ProxyConst.COMMAND_RANGE_QUERY);
		sendToServer(table);
		sendToServer(queryRectangle.toCompactString());

		return new ArrayList<>();
	}

	/**
	 * The put call
	 * @param tuple
	 * @param table
	 * @throws IOException
	 */
	public synchronized void put(final Tuple tuple, final String table) throws IOException {
		sendToServer(ProxyConst.COMMAND_PUT);
		sendToServer(table);

		waitForServerOkResult();
	}

	/**
	 * Delete the tuple for the given key
	 * @param key
	 * @param table
	 * @throws IOException
	 */
	public synchronized void delete(final String key, final String table) throws IOException {
		sendToServer(ProxyConst.COMMAND_DELETE);
		sendToServer(table);
		sendToServer(key);

		waitForServerOkResult();
	}

	/**
	 * Disconnect from server
	 * @return
	 * @throws IOException
	 */
	public synchronized void disconnect() throws IOException {
		sendToServer(ProxyConst.COMMAND_CLOSE);

		try {
			waitForServerOkResult();
		} catch(Exception e) {
			throw e;
		} finally {
			close();
		}
	}
 }
