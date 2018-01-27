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
package org.bboxdb.tools.cli;

import java.util.Arrays;
import java.util.List;

public class CLIAction {

	/**
	 * The name of the import action
	 */
	protected static final String IMPORT = "import";
	
	/**
	 * The name of the query action
	 */
	protected static final String QUERY = "query";
	
	/**
	 * The name of the join action
	 */
	protected static final String JOIN = "join";
		
	/**
	 * The name of the query action
	 */
	protected static final String CONTINUOUS_QUERY = "continuous-query";
	
	/**
	 * The name of the insert action
	 */
	protected static final String INSERT = "insert";
	
	/**
	 * The name of the delete action
	 */
	protected static final String DELETE = "delete";
	
	/**
	 * The name of the create distribution group action
	 */
	protected static final String CREATE_DGROUP = "create_dgroup";
	
	/**
	 * The name of the delete distribution group action
	 */
	protected static final String DELETE_DGROUP = "delete_dgroup";
	
	/**
	 * Create a new table
	 */
	protected static final String CREATE_TABLE = "create_table";
	
	/**
	 * Delete a table
	 */
	protected static final String DELETE_TABLE = "delete_table";
	
	/**
	 * The name of the show distribution group action
	 */
	protected static final String SHOW_DGROUP = "show_dgroup";
	
	/**
	 * The name of the show instances action
	 */
	protected static final String SHOW_INSTANCES = "show_instances";
	
	/**
	 * All known actions
	 */
	protected static List<String> ALL_ACTIONS 
		= Arrays.asList(IMPORT, QUERY, JOIN, DELETE, INSERT, CREATE_DGROUP, DELETE_DGROUP, 
				SHOW_DGROUP, SHOW_INSTANCES, CREATE_TABLE, DELETE_TABLE);

}
