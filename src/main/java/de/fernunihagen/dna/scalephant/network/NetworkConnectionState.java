/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.network;

public enum NetworkConnectionState {

	/**
	 * The states of a network connection
	 */
	
	/**
	 * The connection is establishing
	 */
	NETWORK_CONNECTION_HANDSHAKING,
	
	/**
	 * The connection is open
	 */
	NETWORK_CONNECTION_OPEN,
	
	/**
	 * The connection is closing
	 */
	NETWORK_CONNECTION_CLOSING,
	
	/**
	 * The connection is closed
	 */
	NETWORK_CONNECTION_CLOSED,
	
	/**
	 * The connection is closed with errors
	 */
	NETWORK_CONNECTION_CLOSED_WITH_ERRORS;	
}
