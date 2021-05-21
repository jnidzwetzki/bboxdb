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
package org.bboxdb.network.server.connection;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ClientConnectionRegistry {
	
	private final Set<ClientConnectionHandler> activeConnections;
	
	public ClientConnectionRegistry() {
		this.activeConnections = new HashSet<>();
	}
	
	/**
	 * Register a new client connection
	 * @param handler
	 */
	public synchronized void registerClientConnection(final ClientConnectionHandler handler) {
		activeConnections.add(handler);
	}
	
	/**
	 * Unregister a client connection
	 * @param handler
	 * @return
	 */
	public synchronized boolean deregisterClientConnection(final ClientConnectionHandler handler) {
		return activeConnections.remove(handler);
	}
	
	/**
	 * Get all active connections
	 * @return
	 */
	public synchronized Set<ClientConnectionHandler> getAllActiveConnections() {
		return Collections.unmodifiableSet(activeConnections);
	}
}
