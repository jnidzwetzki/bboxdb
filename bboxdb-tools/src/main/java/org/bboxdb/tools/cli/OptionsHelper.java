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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.bboxdb.misc.Const;

public class OptionsHelper {
	
	/**
	 * Build the command line options
	 * @return
	 */
	public static Options buildOptions() {
		final Options options = new Options();

		// Help
		final Option help = Option.builder(CLIParameter.HELP)
				.desc("Show this help")
				.build();
		options.addOption(help);

		// Be verbose
		final Option verbose = Option.builder(CLIParameter.VERBOSE)
				.desc("Be verbose")
				.build();
		options.addOption(verbose);

		// Action
		final Option action = Option.builder(CLIParameter.ACTION)
				.hasArg()
				.argName("action")
				.desc("The CLI action to execute")
				.build();
		options.addOption(action);

		// Host
		final Option host = Option.builder(CLIParameter.ZOOKEEPER_HOST)
				.hasArg()
				.argName("zookeeperhost")
				.desc("The Zookeeper endpoint to connect to (default: 127.0.0.1:2181)")
				.build();
		options.addOption(host);

		// Cluster name
		final Option clusterName = Option.builder(CLIParameter.ZOOKEEPER_CLUSTER_NAME)
				.hasArg()
				.argName("clustername")
				.desc("The name of the cluster (default: mycluster)")
				.build();
		options.addOption(clusterName);

		// Distribution group
		final Option distributionGroup = Option.builder(CLIParameter.DISTRIBUTION_GROUP)
				.hasArg()
				.argName("distributiongroup")
				.desc("The distribution group")
				.build();
		options.addOption(distributionGroup);

		// Dimensions
		final Option dimensions = Option.builder(CLIParameter.DIMENSIONS)
				.hasArg()
				.argName("dimensions")
				.desc("The number of dimensions")
				.build();
		options.addOption(dimensions);

		// Replication factor
		final Option replicationFactor = Option.builder(CLIParameter.REPLICATION_FACTOR)
				.hasArg()
				.argName("replicationfactor")
				.desc("The replication factor")
				.build();
		options.addOption(replicationFactor);

		// Max region size
		final Option maxRegionSize = Option.builder(CLIParameter.MAX_REGION_SIZE)
				.hasArg()
				.argName("max region size (in MB)")
				.desc("Default: " + Const.DEFAULT_MAX_REGION_SIZE_IN_MB)
				.build();
		options.addOption(maxRegionSize);

		// Min region size
		final Option minRegionSize = Option.builder(CLIParameter.MIN_REGION_SIZE)
				.hasArg()
				.argName("min region size (in MB)")
				.desc("Default: " + Const.DEFAULT_MIN_REGION_SIZE_IN_MB)
				.build();
		options.addOption(minRegionSize);

		// Resource placement
		final Option resourcePlacement = Option.builder(CLIParameter.RESOURCE_PLACEMENT)
				.hasArg()
				.argName("ressource placement")
				.desc("Default: " + Const.DEFAULT_PLACEMENT_STRATEGY)
				.build();
		options.addOption(resourcePlacement);

		// Resource placement config
		final Option resourcePlacementConfig = Option.builder(CLIParameter.RESOURCE_PLACEMENT_CONFIG)
				.hasArg()
				.argName("ressource placement config")
				.desc("Default: " + Const.DEFAULT_PLACEMENT_CONFIG)
				.build();
		options.addOption(resourcePlacementConfig);

		// Space partitioner
		final Option spacePartitioner = Option.builder(CLIParameter.SPACE_PARTITIONER)
				.hasArg()
				.argName("space partitioner")
				.desc("Default: " + Const.DEFAULT_SPACE_PARTITIONER)
				.build();
		options.addOption(spacePartitioner);

		// Space partitioner
		final Option spacePartitionerConfig = Option.builder(CLIParameter.SPACE_PARTITIONER_CONFIG)
				.hasArg()
				.argName("space partitioner configuration")
				.desc("Default: " + Const.DEFAULT_SPACE_PARTITIONER_CONFIG)
				.build();
		options.addOption(spacePartitionerConfig);

		// Table duplicates
		final Option duplplicatesInTable = Option.builder(CLIParameter.DUPLICATES)
				.hasArg()
				.argName("duplicates")
				.desc("Allow duplicates in the table, default: false")
				.build();
		options.addOption(duplplicatesInTable);

		// Table ttl
		final Option ttlForTable = Option.builder(CLIParameter.TTL)
				.hasArg()
				.argName("ttl")
				.desc("The TTL of the tuple versions in milliseconds")
				.build();
		options.addOption(ttlForTable);

		// Table versions
		final Option versionsForTable = Option.builder(CLIParameter.VERSIONS)
				.hasArg()
				.argName("versions")
				.desc("The amount of versions for a tuple")
				.build();
		options.addOption(versionsForTable);

		// Filename
		final Option file = Option.builder(CLIParameter.FILE)
				.hasArg()
				.argName("file")
				.desc("The file to read")
				.build();
		options.addOption(file);

		// Format
		final Option format = Option.builder(CLIParameter.FORMAT)
				.hasArg()
				.argName("format")
				.desc("The format of the file")
				.build();
		options.addOption(format);

		// Table
		final Option table = Option.builder(CLIParameter.TABLE)
				.hasArg()
				.argName("table")
				.desc("The table to carry out the action")
				.build();
		options.addOption(table);

		// Key
		final Option key = Option.builder(CLIParameter.KEY)
				.hasArg()
				.argName("key")
				.desc("The name of the key")
				.build();
		options.addOption(key);

		// BBox
		final Option bbox = Option.builder(CLIParameter.BOUNDING_BOX)
				.hasArg()
				.argName("bounding box")
				.desc("The bounding box of the tuple")
				.build();
		options.addOption(bbox);

		// BBox padding
		final Option bboxPadding = Option.builder(CLIParameter.BOUNDING_BOX_PADDING)
				.hasArg()
				.argName("bounding box padding")
				.desc("The bounding box padding")
				.build();
		options.addOption(bboxPadding);

		// Value
		final Option value = Option.builder(CLIParameter.VALUE)
				.hasArg()
				.argName("value")
				.desc("The value of the tuple")
				.build();
		options.addOption(value);

		// Time
		final Option time = Option.builder(CLIParameter.TIMESTAMP)
				.hasArg()
				.argName("timestamp")
				.desc("The version time stamp of the tuple")
				.build();
		options.addOption(time);
		
		// Custom filter class
		final Option filterclass = Option.builder(CLIParameter.CUSTOM_FILTER_CLASS)
				.hasArg()
				.argName("filterclass")
				.desc("The classname of the custom filter")
				.build();
		options.addOption(filterclass);
		
		// Custom filter value
		final Option filtervalue = Option.builder(CLIParameter.CUSTOM_FILTER_VALUE)
				.hasArg()
				.argName("filtervalue")
				.desc("The value for the custom filter")
				.build();
		options.addOption(filtervalue);
		

		//  Number of partitions
		final Option partitions = Option.builder(CLIParameter.PARTITIONS)
				.hasArg()
				.argName("partitions")
				.desc("The number of partitions in the prepartitions")
				.build();
		options.addOption(partitions);

		return options;
	}
}
