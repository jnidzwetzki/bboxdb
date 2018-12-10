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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.UnknownHostException;

import org.bboxdb.commons.CloseableHelper;

public class NetworkProxyClient implements AutoCloseable {
	
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

	public NetworkProxyClient(final String hostname, final int port) 
			throws UnknownHostException, IOException {
		
		this.clientSocket = new Socket(hostname, port);
		this.socketReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		this.socketWriter = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
	}

	@Override
	public void close() throws IOException {
		CloseableHelper.closeWithoutException(socketReader);
		CloseableHelper.closeWithoutException(socketWriter);
		CloseableHelper.closeWithoutException(clientSocket);
	}
	
	/**
	 * Disconnect from server
	 * @throws IOException 
	 */
	public void disconnect() throws IOException {
		sendToServer("CLOSE");
	}

	/**
	 * Send data to server
	 * @throws IOException 
	 */
	private synchronized void sendToServer(final String string) throws IOException {
		socketWriter.write(string);
		socketWriter.flush();
	}
}
