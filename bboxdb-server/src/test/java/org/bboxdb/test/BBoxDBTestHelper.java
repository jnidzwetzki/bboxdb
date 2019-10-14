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
package org.bboxdb.test;

import java.util.HashSet;
import java.util.Set;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.BBoxDBInstanceManager;
import org.bboxdb.distribution.membership.BBoxDBInstanceState;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.membership.ZookeeperBBoxDBInstanceAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;

public class BBoxDBTestHelper {
	
	/**
	 * Register the local instance in zookeeper
	 * @return
	 * @throws ZookeeperException
	 */
	public static void registerLocalInstance() throws ZookeeperException {
		
		final BBoxDBInstance instance = ZookeeperClientFactory.getLocalInstanceName();
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		
		final ZookeeperBBoxDBInstanceAdapter zookeeperBBoxDBInstanceAdapter 
			= new ZookeeperBBoxDBInstanceAdapter(zookeeperClient);
		
		instance.setState(BBoxDBInstanceState.READY);
		
		zookeeperBBoxDBInstanceAdapter.updateNodeInfo(instance);
		zookeeperBBoxDBInstanceAdapter.updateStateData(instance);
		
		// Register instance
		final Set<BBoxDBInstance> instances = new HashSet<>();
		instances.add(instance);
		BBoxDBInstanceManager.getInstance().updateInstanceList(instances);
		
		// Allow connections to localhost and clear blacklist
		MembershipConnectionService.getInstance().clearBlacklist();
	}
}
