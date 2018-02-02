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
package org.bboxdb.distribution;

import java.util.HashMap;
import java.util.Map;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributionGroupConfigurationCache {

	/**
	 * The instance
	 */
	protected static DistributionGroupConfigurationCache instance;
	
	/**
	 * The cache
	 */
	protected final Map<String, DistributionGroupConfiguration> cache;
	

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionGroupConfigurationCache.class);
	
	
	static {
		instance = new DistributionGroupConfigurationCache();
	}
	
	private DistributionGroupConfigurationCache() {
		// private singleton constructor
		cache = new HashMap<>();
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		throw new IllegalArgumentException("Unable to clone a singleton");
	}
	
	/**
	 * Return the instance
	 * @return
	 */
	public static DistributionGroupConfigurationCache getInstance() {
		return instance;
	}
	
	/**
	 * Get the distribution group configuration
	 * @param distributionGroupName
	 * @return
	 * @throws ZookeeperNotFoundException 
	 */
	public synchronized DistributionGroupConfiguration getDistributionGroupConfiguration(
			final DistributionGroupName distributionGroupName) throws ZookeeperNotFoundException {
		
		return getDistributionGroupConfiguration(distributionGroupName.getFullname());
	}
	
	/**
	 * Get the distribution group configuration
	 * @param distributionGroupName
	 * @return
	 * @throws ZookeeperNotFoundException 
	 */
	public synchronized DistributionGroupConfiguration getDistributionGroupConfiguration(
			final String distributionGroupName) throws ZookeeperNotFoundException {
		
		if(! cache.containsKey(distributionGroupName)) {
			try {
				final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
				final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter = new DistributionGroupZookeeperAdapter(zookeeperClient);
				
				final DistributionGroupConfiguration configuration = distributionGroupZookeeperAdapter.getDistributionGroupConfiguration(distributionGroupName);
				
				addNewConfiguration(distributionGroupName, configuration);
			} catch (InputParseException | ZookeeperException e) {
				logger.error("Exception while reading zokeeper data", e);
				return new DistributionGroupConfiguration();
			} 
		}
		
		return cache.get(distributionGroupName);
	}

	/**
	 * @param distributionGroupName
	 * @param configuration
	 */
	public synchronized void addNewConfiguration(final String distributionGroupName,
			final DistributionGroupConfiguration configuration) {
		
		cache.put(distributionGroupName, configuration);
	}
	
	/**
	 * Clear the cache
	 */
	public synchronized void clear() {
		cache.clear();
	}

}
