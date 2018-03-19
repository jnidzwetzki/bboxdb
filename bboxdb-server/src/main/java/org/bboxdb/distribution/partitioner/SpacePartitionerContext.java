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
package org.bboxdb.distribution.partitioner;

import java.util.Objects;
import java.util.Set;

import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;

public class SpacePartitionerContext {
	
	/**
	 * The space partitioner configuration
	 */
	private String spacePartitionerConfig;
	
	/**
	 * The distribution group name
	 */
	private String distributionGroupName;
	
	/**
	 * The zookeeper client
	 */
	private ZookeeperClient zookeeperClient;
	
	/**
	 * The callbacks
	 */
	private Set<DistributionRegionCallback> callback;
	
	/**
	 * The mapper
	 */
	private DistributionRegionIdMapper mapper;

	public SpacePartitionerContext(final String spacePartitionerConfig, final String distributionGroupName,
			final ZookeeperClient zookeeperClient, final Set<DistributionRegionCallback> callback, 
			final DistributionRegionIdMapper mapper) {
		
		this.spacePartitionerConfig = Objects.requireNonNull(spacePartitionerConfig);
		this.distributionGroupName = Objects.requireNonNull(distributionGroupName);
		this.zookeeperClient = Objects.requireNonNull(zookeeperClient);
		this.callback = Objects.requireNonNull(callback);
		this.mapper =Objects.requireNonNull(mapper);
	}

	public String getSpacePartitionerConfig() {
		return spacePartitionerConfig;
	}

	public String getDistributionGroupName() {
		return distributionGroupName;
	}

	public ZookeeperClient getZookeeperClient() {
		return zookeeperClient;
	}

	public Set<DistributionRegionCallback> getCallbacks() {
		return callback;
	}

	public DistributionRegionIdMapper getDistributionRegionMapper() {
		return mapper;
	}
}