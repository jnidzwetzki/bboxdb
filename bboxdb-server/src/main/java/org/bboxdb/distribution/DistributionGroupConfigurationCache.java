/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
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
	private static DistributionGroupConfigurationCache instance;
	
	/**
	 * The cache
	 */
	protected final Map<String, DistributionGroupConfiguration> cache;

	/**
	 * The lock used to guard the cache
	 */
	private final Object lock = new Object();


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
		throw new CloneNotSupportedException("Unable to clone a singleton");
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
	public DistributionGroupConfiguration getDistributionGroupConfiguration(
			final String distributionGroupName) throws ZookeeperNotFoundException {

		synchronized (lock) {
			if(! cache.containsKey(distributionGroupName)) {
				try {
					final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
					final DistributionGroupAdapter distributionGroupZookeeperAdapter = new DistributionGroupAdapter(zookeeperClient);

					final DistributionGroupConfiguration configuration = distributionGroupZookeeperAdapter.getDistributionGroupConfiguration(distributionGroupName);

					addNewConfiguration(distributionGroupName, configuration);
				} catch (InputParseException | ZookeeperException e) {
					logger.error("Exception while reading zokeeper data", e);
					return new DistributionGroupConfiguration();
				}
			}

			return cache.get(distributionGroupName);
		}
	}

	/**
	 * @param distributionGroupName
	 * @param configuration
	 */
	public void addNewConfiguration(final String distributionGroupName,
			final DistributionGroupConfiguration configuration) {

		synchronized (lock) {
			cache.put(distributionGroupName, configuration);
		}
	}

	/**
	 * Clear the cache
	 */
	public void clear() {
		synchronized (lock) {
			cache.clear();
		}
	}

}
