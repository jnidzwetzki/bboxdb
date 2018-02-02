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

import org.bboxdb.misc.Const;

public class DistributionGroupConfiguration {

	/**
	 * The dimension
	 */
	protected int dimensions = -1;
	
	/** 
	 * The replication factor
	 */
	protected short replicationFactor = 3;
	
	/**
	 * The maximal region size
	 */
	protected int maximumRegionSize = Const.DEFAULT_MAX_REGION_SIZE;
	
	/**
	 * The minimal region size
	 */
	protected int minimumRegionSize = Const.DEFAULT_MIN_REGION_SIZE;
	
	/**
	 * The default placement strategy
	 */
	protected String placementStrategy = Const.DEFAULT_PLACEMENT_STRATEGY;
	
	/**
	 * The placement strategy config
	 */
	protected String placementStrategyConfig = Const.DEFAULT_PLACEMENT_CONFIG;
	
	/**
	 * The space paritioner
	 */
	protected String spacePartitioner = Const.DEFAULT_SPACE_PARTITIONER;
	
	/**
	 * The space partitioner config
	 */
	protected String spacePartitionerConfig = Const.DEFAULT_SPACE_PARTITIONER_CONFIG;

	public DistributionGroupConfiguration() {
	}

	public DistributionGroupConfiguration(final int dimensions) {
		this.dimensions = dimensions;
	}

	public short getReplicationFactor() {
		return replicationFactor;
	}

	public void setReplicationFactor(final short replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	public int getMaximumRegionSize() {
		return maximumRegionSize;
	}

	public void setMaximumRegionSize(final int maximalRegionSize) {
		this.maximumRegionSize = maximalRegionSize;
	}
	
	public int getMinimumRegionSize() {
		return minimumRegionSize;
	}

	public void setMinimumRegionSize(final int minimalRegionSize) {
		this.minimumRegionSize = minimalRegionSize;
	}

	public String getPlacementStrategy() {
		return placementStrategy;
	}

	public void setPlacementStrategy(final String placementStrategy) {
		this.placementStrategy = placementStrategy;
	}

	public String getPlacementStrategyConfig() {
		return placementStrategyConfig;
	}

	public void setPlacementStrategyConfig(final String placementStrategyConfig) {
		this.placementStrategyConfig = placementStrategyConfig;
	}

	public String getSpacePartitioner() {
		return spacePartitioner;
	}

	public void setSpacePartitioner(final String spacePartitioner) {
		this.spacePartitioner = spacePartitioner;
	}

	public String getSpacePartitionerConfig() {
		return spacePartitionerConfig;
	}

	public void setSpacePartitionerConfig(final String spacePartitionerConfig) {
		this.spacePartitionerConfig = spacePartitionerConfig;
	}

	public int getDimensions() {
		return dimensions;
	}

	public void setDimensions(final int dimensions) {
		this.dimensions = dimensions;
	}

	@Override
	public String toString() {
		return "DistributionGroupConfiguration [dimensions=" + dimensions + ", replicationFactor=" + replicationFactor
				+ ", maximumRegionSize=" + maximumRegionSize + ", minimumRegionSize=" + minimumRegionSize
				+ ", placementStrategy=" + placementStrategy + ", placementStrategyConfig=" + placementStrategyConfig
				+ ", spacePartitioner=" + spacePartitioner + ", spacePartitionerConfig=" + spacePartitionerConfig + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + dimensions;
		result = prime * result + maximumRegionSize;
		result = prime * result + minimumRegionSize;
		result = prime * result + ((placementStrategy == null) ? 0 : placementStrategy.hashCode());
		result = prime * result + ((placementStrategyConfig == null) ? 0 : placementStrategyConfig.hashCode());
		result = prime * result + replicationFactor;
		result = prime * result + ((spacePartitioner == null) ? 0 : spacePartitioner.hashCode());
		result = prime * result + ((spacePartitionerConfig == null) ? 0 : spacePartitionerConfig.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DistributionGroupConfiguration other = (DistributionGroupConfiguration) obj;
		if (dimensions != other.dimensions)
			return false;
		if (maximumRegionSize != other.maximumRegionSize)
			return false;
		if (minimumRegionSize != other.minimumRegionSize)
			return false;
		if (placementStrategy == null) {
			if (other.placementStrategy != null)
				return false;
		} else if (!placementStrategy.equals(other.placementStrategy))
			return false;
		if (placementStrategyConfig == null) {
			if (other.placementStrategyConfig != null)
				return false;
		} else if (!placementStrategyConfig.equals(other.placementStrategyConfig))
			return false;
		if (replicationFactor != other.replicationFactor)
			return false;
		if (spacePartitioner == null) {
			if (other.spacePartitioner != null)
				return false;
		} else if (!spacePartitioner.equals(other.spacePartitioner))
			return false;
		if (spacePartitionerConfig == null) {
			if (other.spacePartitionerConfig != null)
				return false;
		} else if (!spacePartitionerConfig.equals(other.spacePartitionerConfig))
			return false;
		return true;
	}

	
}
