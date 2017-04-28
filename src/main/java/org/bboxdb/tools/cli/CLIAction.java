/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
	protected static final String CREATE_DGROUP = "create_distribution_group";
	
	/**
	 * The name of the delete distribution group action
	 */
	protected static final String DELETE_DGROUP = "delete_distribution_group";

	/**
	 * All known actions
	 */
	protected List<String> ALL_ACTIONS 
		= Arrays.asList(IMPORT, QUERY, CREATE_DGROUP, DELETE_DGROUP);

}
