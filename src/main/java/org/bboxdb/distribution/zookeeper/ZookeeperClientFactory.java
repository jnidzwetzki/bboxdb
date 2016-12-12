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
package org.bboxdb.distribution.zookeeper;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.bboxdb.Const;
import org.bboxdb.BBoxDBConfiguration;
import org.bboxdb.BBoxDBConfigurationManager;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;

public class ZookeeperClientFactory {
	
	protected final static Map<BBoxDBConfiguration, ZookeeperClient> instances;
	
	static {
		instances = new HashMap<BBoxDBConfiguration, ZookeeperClient>();
	}
	
	/**
	 * Returns a new instance of the zookeeper client 
	 * @return
	 */
	public static ZookeeperClient getZookeeperClient() {
		final BBoxDBConfiguration scalephantConfiguration = 
				BBoxDBConfigurationManager.getConfiguration();
		
		return getZookeeperClient(scalephantConfiguration);
	}

	/**
	 * Returns a new instance of the zookeeper client (with a specific configuration)
	 * @param scalephantConfiguration
	 * @return
	 */
	public static ZookeeperClient getZookeeperClient(
			final BBoxDBConfiguration scalephantConfiguration) {
		
		// Is an instance for the configuration known?
		if(instances.containsKey(scalephantConfiguration)) {
			return instances.get(scalephantConfiguration);
		}
		
		final Collection<String> zookeepernodes = scalephantConfiguration.getZookeepernodes();
		final String clustername = scalephantConfiguration.getClustername();

		final ZookeeperClient zookeeperClient = new ZookeeperClient(zookeepernodes, clustername);
		final DistributedInstance instance = getLocalInstanceName(scalephantConfiguration);
		zookeeperClient.registerScalephantInstanceAfterConnect(instance);
		
		// Register instance
		instances.put(scalephantConfiguration, zookeeperClient);
		
		return zookeeperClient;
	}

	/**
	 * Get the name of the local instance
	 * @param scalephantConfiguration
	 * @return
	 */
	public static DistributedInstance getLocalInstanceName(final BBoxDBConfiguration scalephantConfiguration) {
		final String localIp = scalephantConfiguration.getLocalip();
		final int localPort = scalephantConfiguration.getNetworkListenPort();
		final DistributedInstance instance = new DistributedInstance(localIp, localPort, Const.VERSION);
		return instance;
	}

	/**
	 * Get a zookeeoer instance and init if needed
	 * @return
	 */
	public static ZookeeperClient getZookeeperClientAndInit() {
		final ZookeeperClient zookeeperClient = getZookeeperClient();
		
		if(! zookeeperClient.isConnected()) {
			zookeeperClient.init();
		}
		
		return zookeeperClient;
	}
	
	/**
	 * Get a new instance of the DistributionGroupZookeeperAdapter
	 */
	public static DistributionGroupZookeeperAdapter getDistributionGroupAdapter() {
		final ZookeeperClient zookeeperClient = getZookeeperClient();
		return new DistributionGroupZookeeperAdapter(zookeeperClient);
	}
}
