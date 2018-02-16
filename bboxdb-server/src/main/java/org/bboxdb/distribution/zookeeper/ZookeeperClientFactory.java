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

import java.util.Collection;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.Const;

public class ZookeeperClientFactory {
	
	private static ZookeeperClient client;
	
	/**
	 * The name of the local instance
	 */
	private static BBoxDBInstance localInstanceName;

	/**
	 * Returns a new instance of the zookeeper client 
	 * @return
	 */
	public synchronized static ZookeeperClient getZookeeperClient() {
		
		if(client == null) {
			final BBoxDBConfiguration bboxdbConfiguration = 
					BBoxDBConfigurationManager.getConfiguration();
	
			final Collection<String> zookeepernodes = bboxdbConfiguration.getZookeepernodes();
			final String clustername = bboxdbConfiguration.getClustername();
	
			client = new ZookeeperClient(zookeepernodes, clustername);
		
			if(! client.isConnected()) {
				client.init();
			}
		}
		
		return client;
	}
	
	/**
	 * Set the default zookeeper client
	 * @param zookeeperClient
	 */
	public static synchronized void setDefaultZookeeperClient(final ZookeeperClient zookeeperClient) {
		if(client != null) {
			throw new RuntimeException("Unable to set zookeeper client, already set");
		}
		
		client = zookeeperClient;
	}

	/**
	 * Get the name of the local instance
	 * @param bboxdbConfiguration
	 * @return
	 */
	public static synchronized BBoxDBInstance getLocalInstanceName() {
		
		if(localInstanceName == null) {
			final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();
			final String localIp = configuration.getLocalip();
			final int localPort = configuration.getNetworkListenPort();
			localInstanceName = new BBoxDBInstance(localIp, localPort, Const.VERSION);
		}
		
		return localInstanceName;
	}
	
	/**
	 * Get a new instance of the DistributionGroupZookeeperAdapter
	 */
	public static DistributionGroupZookeeperAdapter getDistributionGroupAdapter() {
		final ZookeeperClient zookeeperClient = getZookeeperClient();
		return new DistributionGroupZookeeperAdapter(zookeeperClient);
	}
}
