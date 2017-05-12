/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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

import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.Const;

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
		final BBoxDBConfiguration configuration = 
				BBoxDBConfigurationManager.getConfiguration();
		
		return getZookeeperClient(configuration);
	}

	/**
	 * Returns a new instance of the zookeeper client (with a specific configuration)
	 * @param bboxdbConfiguration
	 * @return
	 */
	public static ZookeeperClient getZookeeperClient(
			final BBoxDBConfiguration bboxdbConfiguration) {
		
		// Is an instance for the configuration known?
		if(instances.containsKey(bboxdbConfiguration)) {
			return instances.get(bboxdbConfiguration);
		}
		
		final Collection<String> zookeepernodes = bboxdbConfiguration.getZookeepernodes();
		final String clustername = bboxdbConfiguration.getClustername();

		final ZookeeperClient zookeeperClient = new ZookeeperClient(zookeepernodes, clustername);
		final DistributedInstance instance = getLocalInstanceName(bboxdbConfiguration);
		zookeeperClient.registerInstanceAfterConnect(instance);
		
		// Register instance
		instances.put(bboxdbConfiguration, zookeeperClient);
		
		if(! zookeeperClient.isConnected()) {
			zookeeperClient.init();
		}
		
		return zookeeperClient;
	}

	/**
	 * Get the name of the local instance
	 * @param bboxdbConfiguration
	 * @return
	 */
	public static DistributedInstance getLocalInstanceName(final BBoxDBConfiguration bboxdbConfiguration) {
		final String localIp = bboxdbConfiguration.getLocalip();
		final int localPort = bboxdbConfiguration.getNetworkListenPort();
		final DistributedInstance instance = new DistributedInstance(localIp, localPort, Const.VERSION);
		return instance;
	}
	
	/**
	 * Get a new instance of the DistributionGroupZookeeperAdapter
	 */
	public static DistributionGroupZookeeperAdapter getDistributionGroupAdapter() {
		final ZookeeperClient zookeeperClient = getZookeeperClient();
		return new DistributionGroupZookeeperAdapter(zookeeperClient);
	}
}
