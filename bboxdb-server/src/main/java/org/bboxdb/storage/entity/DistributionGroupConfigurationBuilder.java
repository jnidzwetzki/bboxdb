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
package org.bboxdb.storage.entity;

public class DistributionGroupConfigurationBuilder {

	/**
	 * The distribution group configuration
	 */
	protected final DistributionGroupConfiguration distributionGroupConfiguration;
	
	protected DistributionGroupConfigurationBuilder(final int dimensions) {
		distributionGroupConfiguration = new DistributionGroupConfiguration();
		distributionGroupConfiguration.setDimensions(dimensions);
	}
	
	/**
	 * Create a new configuration builder
	 * @return
	 */
	public static DistributionGroupConfigurationBuilder create(final int dimensions) {
		final DistributionGroupConfigurationBuilder builder = new DistributionGroupConfigurationBuilder(dimensions);
		return builder;
	}
	
	/**
	 * Set the replication factor
	 * @param replicationFactor
	 * @return
	 */
	public DistributionGroupConfigurationBuilder withReplicationFactor(final short replicationFactor) {
		distributionGroupConfiguration.setReplicationFactor(replicationFactor);
		return this;
	}
	
	/**
	 * Set the maximal region size
	 * @param maximumRegionSize
	 * @return
	 */
	public DistributionGroupConfigurationBuilder withMaximumRegionSize(final int maximumRegionSize) {
		distributionGroupConfiguration.setMaximumRegionSize(maximumRegionSize);
		return this;
	}
	
	/**
	 * Set the minimal region size
	 * @param minimumRegionSize
	 * @return
	 */
	public DistributionGroupConfigurationBuilder withMinimumRegionSize(final int minimumRegionSize) {
		distributionGroupConfiguration.setMinimumRegionSize(minimumRegionSize);
		return this;
	}
	
	/**
	 * Set the placement strategy
	 * @param placementStrategy
	 * @param config
	 * @return
	 */
	public DistributionGroupConfigurationBuilder withPlacementStrategy(final String placementStrategy, 
			final String config) {
		
		distributionGroupConfiguration.setPlacementStrategy(placementStrategy);
		distributionGroupConfiguration.setPlacementStrategyConfig(config);
		return this;
	}
	
	/**
	 * Set the space partitioner
	 * @param spacePartitioner
	 * @param config
	 * @return
	 */
	public DistributionGroupConfigurationBuilder withSpacePartitioner(final String spacePartitioner,
			final String config) {
		
		distributionGroupConfiguration.setSpacePartitioner(spacePartitioner);
		distributionGroupConfiguration.setPlacementStrategyConfig(config);
		return this;
	}
	
	/**
	 * Return the resulting configuration object
	 * @return
	 */
	public DistributionGroupConfiguration build() {
		return distributionGroupConfiguration;
	}

}
