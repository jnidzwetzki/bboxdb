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

public class CLIParameter {

	/**
	 * The zookeeper host
	 */
	protected static final String HOST = "host";

	/**
	 * The name of the cluster
	 */
	protected static final String CLUSTER_NAME = "cluster";
	
	/**
	 * The name of the action parameter
	 */
	protected static final String ACTION = "action";
	
	/**
	 * The name of the help parameter
	 */
	protected static final String HELP = "help";
	
	/**
	 * Verbose flag
	 */
	protected static final String VERBOSE = "verbose";
	
	/**
	 * The distribution group
	 */
	protected static final String DISTRIBUTION_GROUP = "distributiongroup";
	
	/**
	 * Replication factor
	 */
	protected static final String REPLICATION_FACTOR = "replicationFactor";
	
	/**
	 * Filename
	 */
	protected static final String FILE  = "file";
	
}
