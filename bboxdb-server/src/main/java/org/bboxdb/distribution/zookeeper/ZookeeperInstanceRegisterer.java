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

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.ZookeeperBBoxDBInstanceAdapter;
import org.bboxdb.misc.BBoxDBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperInstanceRegisterer implements BBoxDBService {
	
	/**
	 * The name of the instance
	 */
	protected final BBoxDBInstance instance;
	
	/**
	 * The zookeeper client
	 */
	private final ZookeeperClient zookeeperClient;

	public ZookeeperInstanceRegisterer() {
		this.instance = ZookeeperClientFactory.getLocalInstanceName();
		this.zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
	}
	
	public ZookeeperInstanceRegisterer(final BBoxDBInstance instance, final ZookeeperClient zookeeperClient) {
		this.instance = instance;
		this.zookeeperClient = zookeeperClient;
	}
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);
	
	@Override
	public void init() {
		
		if (instance == null) {
			logger.error("Unable to determine local instance name");
			return;
		}
		
		try {
			final ZookeeperBBoxDBInstanceAdapter zookeeperBBoxDBInstanceAdapter 
				= new ZookeeperBBoxDBInstanceAdapter(zookeeperClient);
			
			zookeeperBBoxDBInstanceAdapter.updateNodeInfo(instance);
			zookeeperBBoxDBInstanceAdapter.updateStateData(instance);
			
		} catch (ZookeeperException e) {
			logger.error("Exception while registering instance", e);
		}
	}	

	@Override
	public void shutdown() {
		// Do nothing
	}

	@Override
	public String getServicename() {
		return "Instance registerer for: " + instance;
	}
}


