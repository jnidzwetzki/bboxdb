/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.distribution;

import java.util.concurrent.atomic.AtomicInteger;

import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;

public class DistributionRegionFactory {

	/**
	 * The instance to the zookeeper client
	 */
	protected static ZookeeperClient zookeeperClient = null;
	
	/**
	 * The base level of a tree
	 */
	protected final static int BASE_LEVEL = 0;
	
	/**
	 * Factory method for a new root region
	 * @param name
	 * @return
	 */
	public static DistributionRegion createRootRegion(final String name) {
		final DistributionGroupName distributionGroupName = new DistributionGroupName(name);
		
		if(! distributionGroupName.isValid()) {
			throw new IllegalArgumentException("Invalid region name: " + name);
		}
		
		DistributionRegion result = null;
		
		if(zookeeperClient != null) {
			result = new DistributionRegionWithZookeeperIntegration(distributionGroupName, 
					BASE_LEVEL, new AtomicInteger(0), zookeeperClient);
		} else {
			result = new DistributionRegion(distributionGroupName, BASE_LEVEL, new AtomicInteger(0));
		}	
		
		result.onNodeComplete();
		
		return result;
	}

	/**
	 * Get the zookeeper instance
	 * @return
	 */
	public static ZookeeperClient getZookeeperClient() {
		return zookeeperClient;
	}

	/**
	 * Set the zookeeper instance
	 * @param zookeeperClient
	 */
	public static void setZookeeperClient(final ZookeeperClient zookeeperClient) {
		DistributionRegionFactory.zookeeperClient = zookeeperClient;
	}	
}
