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
package org.bboxdb.distribution;

import java.util.HashMap;
import java.util.Map;

import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.mode.KDtreeSpacePartitioner;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.entity.TupleStoreName;

public class DistributionGroupCache {
	
	/**
	 * Mapping between the string group and the group object
	 */
	protected final static Map<String, KDtreeSpacePartitioner> groupGroupMap;

	static {
		groupGroupMap = new HashMap<String, KDtreeSpacePartitioner>();
	}
	
	/**
	 * Get the distribution region for the given group name
	 * @param groupName
	 * @return
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	public static synchronized KDtreeSpacePartitioner getGroupForGroupName(final String groupName, 
			final ZookeeperClient zookeeperClient) throws ZookeeperException {
		
		if(! groupGroupMap.containsKey(groupName)) {
			final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter = new DistributionGroupZookeeperAdapter(zookeeperClient);
			final KDtreeSpacePartitioner adapter = distributionGroupZookeeperAdapter.readDistributionGroup(groupName);
			groupGroupMap.put(groupName, adapter);
		}
		
		return groupGroupMap.get(groupName);
	}
	
	/**
	 * Get the distribution region for the given table name
	 * @param groupName
	 * @return
	 * @throws ZookeeperException 
	 * @throws BBoxDBException 
	 * @throws ZookeeperNotFoundException 
	 */
	public static synchronized KDtreeSpacePartitioner getGroupForTableName(
			final TupleStoreName ssTableName, final ZookeeperClient zookeeperClient) 
					throws ZookeeperException, BBoxDBException {
		
		if(! ssTableName.isValid()) {
			throw new BBoxDBException("Invalid tablename: " + ssTableName);
		}
		
		return getGroupForGroupName(ssTableName.getDistributionGroup(), zookeeperClient);
	}
}
