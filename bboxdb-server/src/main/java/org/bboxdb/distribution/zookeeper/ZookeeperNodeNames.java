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
package org.bboxdb.distribution.zookeeper;

public class ZookeeperNodeNames {
	
	/**
	 * The prefix for nodes in sequential queues
	 */
	public final static String SEQUENCE_QUEUE_PREFIX = "id-";
	
	/**
	 * Name of the left tree node
	 */
	public final static String NAME_CHILDREN = "child-";

	/**
	 * Name of the bounding box node
	 */
	public final static String NAME_BOUNDINGBOX = "bbox";
	
	/**
	 * Name of the name prefix node
	 */
	public final static String NAME_NAMEPREFIX = "nameprefix";
	
	/**
	 * Name of the name prefix queue
	 */
	public static final String NAME_PREFIXQUEUE = "nameprefixqueue";

	/**
	 * Name of the dimensions node
	 */
	public final static String NAME_DIMENSIONS = "dimensions";
	
	/**
	 * Name of the replication node
	 */
	public final static String NAME_REPLICATION = "replication";
	
	/**
	 * Name of the systems node
	 */
	public final static String NAME_SYSTEMS = "systems";
	
	/**
	 * Name of the systems node
	 */
	public final static String NAME_STATISTICS = "statistics";
	
	/**
	 * Name of the statistics total tuple node
	 */
	public final static String NAME_STATISTICS_TOTAL_TUPLES = "total_tuples";
	
	/**
	 * Name of the statistics total size node
	 */
	public final static String NAME_STATISTICS_TOTAL_SIZE = "total_size";
	
	/**
	 * Name of the version node
	 */
	public final static String NAME_SYSTEMS_VERSION = "version";
	
	/**
	 * Name of the state node
	 */
	public final static String NAME_SYSTEMS_STATE = "state";
	
	/**
	 * Name of the version node
	 */
	public final static String NAME_NODE_VERSION= "node-version";

	/**
	 * Name of the space partitioner node
	 */
	public static final String NAME_SPACEPARTITIONER = "spacepartitioner";

	/**
	 * Name of the space partitioner config node
	 */
	public static final String NAME_SPACEPARTITIONER_CONFIG = "spacepartitionerconfig";

	/**
	 * Name of the placement strategy node
	 */
	public static final String NAME_PLACEMENT_STRATEGY = "placement";
	
	/**
	 * The placement strategy config
	 */
	public static final String NAME_PLACEMENT_CONFIG = "placementconfig";	

	/**
	 * Name of the max region size node
	 */
	public static final String NAME_MAX_REGION_SIZE = "maxregionsize";
	
	/**
	 * Name of the min region size node
	 */
	public static final String NAME_MIN_REGION_SIZE = "minregionsize";
	
	/**
	 * The tables node name
	 */
	public static final String NAME_TABLES = "tables";
}