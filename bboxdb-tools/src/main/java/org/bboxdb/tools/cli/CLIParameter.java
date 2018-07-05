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
	public static final String ZOOKEEPER_HOST = "host";

	/**
	 * The name of the cluster
	 */
	public static final String ZOOKEEPER_CLUSTER_NAME = "cluster";
	
	/**
	 * The name of the action parameter
	 */
	public static final String ACTION = "action";
	
	/**
	 * The name of the help parameter
	 */
	public static final String HELP = "help";
	
	/**
	 * Verbose flag
	 */
	public static final String VERBOSE = "verbose";
	
	/**
	 * The distribution group
	 */
	public static final String DISTRIBUTION_GROUP = "dgroup";
	
	/**
	 * Replication factor
	 */
	public static final String DIMENSIONS = "dimensions";
	
	/**
	 * Replication factor
	 */
	public static final String REPLICATION_FACTOR = "replicationfactor";
	
	/**
	 * Max region size
	 */
	public static final String MAX_REGION_SIZE = "maxregionsize";
	
	/**
	 * Max region size
	 */
	public static final String MIN_REGION_SIZE = "minregionsize";
	
	/**
	 * Resource placement strategy
	 */
	public static final String RESOURCE_PLACEMENT = "resourceplacement";
	
	/**
	 * Resource placement config
	 */
	public static final String RESOURCE_PLACEMENT_CONFIG = "resourcepconfig";
	
	/**
	 * Space partitioner
	 */
	public static final String SPACE_PARTITIONER = "spacepartitioner";
	
	/**
	 * Space partitioner configuration
	 */
	public static final String SPACE_PARTITIONER_CONFIG = "spacepconfig";
	
	/**
	 * Filename
	 */
	public static final String FILE  = "file";
	
	/**
	 * Input format
	 */
	public static final String FORMAT = "format";
	
	/**
	 * Table
	 */
	public static final String TABLE = "table";
	
	/**
	 * Time
	 */
	public static final String TIMESTAMP = "time";
	
	/**
	 * Key
	 */
	public static final String KEY = "key";
	
	/**
	 * Bounding Box
	 */
	public static final String BOUNDING_BOX = "bbox";
	
	/**
	 * Bounding Box padding
	 */
	public static final String BOUNDING_BOX_PADDING = "bboxpadding";

	/**
	 * Value
	 */
	public static final String VALUE = "value";
	
	/**
	 * Allow duplicates
	 */
	public static final String DUPLICATES = "duplicates";
	
	/**
	 * The TTL for the table
	 */
	public static final String TTL = "ttl";
	
	/**
	 * The amount of tuple versions per table
	 */
	public static final String VERSIONS = "versions";
	
	/**
	 * The name of the spatial index writer
	 */
	public static final String SPATIAL_INDEX_WRITER = "sindexwriter";
	
	/**
	 * The name of the spatial index reader
	 */
	public static final String SPATIAL_INDEX_READER = "sindexreader";
}
