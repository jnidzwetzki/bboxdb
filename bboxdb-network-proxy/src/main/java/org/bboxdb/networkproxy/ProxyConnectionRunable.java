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

import java.net.Socket;

import org.bboxdb.commons.CloseableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyConnectionRunable implements Runnable {

	/**
	 * The client socket
	 */
	private Socket clientSocket;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ProxyConnectionRunable.class);


	public ProxyConnectionRunable(final Socket clientSocket) {
		this.clientSocket = clientSocket;
	}

	@Override
	public void run() {
		logger.info("Closing connection to: {}", clientSocket.getRemoteSocketAddress());
		CloseableHelper.closeWithoutException(clientSocket);
	}

}
