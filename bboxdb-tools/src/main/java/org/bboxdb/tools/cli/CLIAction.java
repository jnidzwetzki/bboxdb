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
package org.bboxdb.tools.cli;

import java.util.Arrays;
import java.util.List;

public class CLIAction {

	/**
	 * The name of the import action
	 */
	public static final String IMPORT = "import";
	
	/**
	 * The name of the query action
	 */
	public static final String QUERY_KEY = "query_key";
	
	/**
	 * The name of the query action
	 */
	public static final String QUERY_RANGE = "query_range";
	
	/**
	 * The name of the query action
	 */
	public static final String QUERY_RANGE_TIME = "query_range_and_time";
	
	/**
	 * The name of the query action
	 */
	public static final String QUERY_TIME = "query_time";
	
	/**
	 * The name of the join action
	 */
	public static final String QUERY_JOIN = "query_join";
		
	/**
	 * The name of the query action
	 */
	public static final String QUERY_CONTINUOUS = "query_continuous";
	
	/**
	 * The name of the insert action
	 */
	public static final String INSERT = "insert";
	
	/**
	 * The name of the delete action
	 */
	public static final String DELETE = "delete";
	
	/**
	 * The name of the create distribution group action
	 */
	public static final String CREATE_DGROUP = "create_dgroup";
	
	/**
	 * The name of the delete distribution group action
	 */
	public static final String DELETE_DGROUP = "delete_dgroup";
	
	/**
	 * Create a new table
	 */
	public static final String CREATE_TABLE = "create_table";
	
	/**
	 * Delete a table
	 */
	public static final String DELETE_TABLE = "delete_table";
	
	/**
	 * The name of the show distribution group action
	 */
	public static final String SHOW_DGROUP = "show_dgroup";
	
	/**
	 * The name of the show instances action
	 */
	public static final String SHOW_INSTANCES = "show_instances";
	
	/**
	 * Execute a prepartitioning
	 */
	public static final String PREPARTITION = "prepartition";
	
	/**
	 * All known actions
	 */
	public static List<String> ALL_ACTIONS 
		= Arrays.asList(IMPORT, QUERY_KEY, QUERY_RANGE, QUERY_RANGE_TIME, QUERY_TIME, QUERY_JOIN, 
				DELETE, INSERT, CREATE_DGROUP, DELETE_DGROUP, SHOW_DGROUP, SHOW_INSTANCES, 
				CREATE_TABLE, DELETE_TABLE, PREPARTITION);

}
