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
	 * Distribution group already exists
	 */
	public final static String ERROR_DGROUP_EXISTS = "Distribution group already exists";
	
	/**
	 * Distribution group invalid name
	 */
	public final static String ERROR_DGROUP_INVALID_NAME = "Invalid name for a distribution group";
	
	/**
	 * Distribution group invalid to allocate resources
	 */
	public static final String ERROR_DGROUP_RESOURCE_PLACEMENT_PROBLEM = "Resource allocation/placement problem";
	
	/**
	 * Table already exists
	 */
	public final static String ERROR_TABLE_EXISTS = "Table already exists";
	
	/**
	 * Table does not exist
	 */
	public final static String ERROR_TABLE_NOT_EXIST = "Table does not exists";
	
	/**
	 * Table with invalid name
	 */
	public final static String ERROR_TABLE_INVALID_NAME = "Invalid name for table";

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
	
	/**
	 * Outdated tuples detected
	 */
	public final static String ERROR_OUTDATED_TUPLES = "Outdated tuples found due to gossip";
	
	/**
	 * Local operation is rejected, retry 
	 */
	public final static String ERROR_LOCAL_OPERATION_REJECTED_RETRY = "Local operation rejected, please retry";

	/**
	 * The locked tuple is outdated
	 */
	public final static String ERROR_LOCK_FAILED_OUTDATED = "Tuple for locking is outdated";

	/**
	 * The tuple is already locked
	 */
	public final static String ERROR_LOCK_FAILED_ALREADY_LOCKED = "Tuple is already locked";
	
	/**
	 * The package is not routed
	 */
	public final static String ERROR_PACKAGE_NOT_ROUTED = "Package is not routed";
	
	/**
	 * The tuple has the wrong dimension for the group
	 */
	public final static String ERROR_TUPLE_HAS_WRONG_DIMENSION = "The tuple has the wrong dimension for the group";

}
