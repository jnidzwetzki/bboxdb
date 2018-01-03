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
package org.bboxdb.network.server;

public class ErrorMessages {
	
	/**
	 * The unsupported package error
	 */
	public final static String ERROR_UNSUPPORTED_PACKAGE_TYPE = "Unsupported package type";

	/**
	 * Exception during query processing
	 */
	public final static String ERROR_EXCEPTION = "Got an exception during query processing, check server logs";

	/**
	 * Query not found error
	 */
	public final static String ERROR_QUERY_NOT_FOUND = "Query not found";
	
	/**
	 * Query not found error
	 */
	public final static String ERROR_QUERY_TO_MUCH = "Client requested to much queries";
	
	/**
	 * Server shutdown
	 */
	public final static String ERROR_QUERY_SHUTDOWN = "Unable to execute query, server started shutdown";

	/**
	 * Routing failed
	 */
	public final static String ERROR_ROUTING_FAILED = "Package routing faield";
}
