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

public class CLIParameter {

	/**
	 * The zookeeper host
	 */
	protected static final String ZOOKEEPER_HOST = "host";

	/**
	 * The name of the cluster
	 */
	protected static final String ZOOKEEPER_CLUSTER_NAME = "cluster";
	
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
	protected static final String DISTRIBUTION_GROUP = "dgroup";
	
	/**
	 * Replication factor
	 */
	protected static final String DIMENSIONS = "dimensions";
	
	/**
	 * Replication factor
	 */
	protected static final String REPLICATION_FACTOR = "replicationfactor";
	
	/**
	 * Max region size
	 */
	protected static final String MAX_REGION_SIZE = "maxregionsize";
	
	/**
	 * Max region size
	 */
	protected static final String MIN_REGION_SIZE = "minregionsize";
	
	/**
	 * Resource placement strategy
	 */
	protected static final String RESOURCE_PLACEMENT = "resourceplacement";
	
	/**
	 * Resource placement config
	 */
	protected static final String RESOURCE_PLACEMENT_CONFIG = "resourcepconfig";
	
	/**
	 * Space partitioner
	 */
	protected static final String SPACE_PARTITIONER = "spacepartitioner";
	
	/**
	 * Space partitioner configuration
	 */
	protected static final String SPACE_PARTITIONER_CONFIG = "spacepconfig";
	
	/**
	 * Filename
	 */
	protected static final String FILE  = "file";
	
	/**
	 * Input format
	 */
	protected static final String FORMAT = "format";
	
	/**
	 * Table
	 */
	protected static final String TABLE = "table";
	
	/**
	 * Time
	 */
	protected static final String TIMESTAMP = "time";
	
	/**
	 * Key
	 */
	protected static final String KEY = "key";
	
	/**
	 * Bounding Box
	 */
	protected static final String BOUNDING_BOX = "bbox";

	/**
	 * Value
	 */
	protected static final String VALUE = "value";
	
	/**
	 * Allow duplicates
	 */
	protected static final String DUPLICATES = "duplicates";
	
	/**
	 * The TTL for the table
	 */
	protected static final String TTL = "ttl";
	
	/**
	 * The amount of tuple versions per table
	 */
	protected static final String VERSIONS = "versions";
	
	/**
	 * The name of the spatial index writer
	 */
	protected static final String SPATIAL_INDEX_WRITER = "sindexwriter";
	
	/**
	 * The name of the spatial index reader
	 */
	protected static final String SPATIAL_INDEX_READER = "sindexreader";
}
