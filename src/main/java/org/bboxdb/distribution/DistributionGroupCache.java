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
package org.bboxdb.distribution;

import java.util.HashMap;
import java.util.Map;

import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.entity.SSTableName;

public class DistributionGroupCache {
	
	/**
	 * Mapping between the string group and the group object
	 */
	protected final static Map<String, KDtreeZookeeperAdapter> groupGroupMap;

	static {
		groupGroupMap = new HashMap<String, KDtreeZookeeperAdapter>();
	}
	
	/**
	 * Get the distribution region for the given group name
	 * @param groupName
	 * @return
	 * @throws ZookeeperException 
	 */
	public static synchronized KDtreeZookeeperAdapter getGroupForGroupName(final String groupName, final ZookeeperClient zookeeperClient) throws ZookeeperException {
		if(! groupGroupMap.containsKey(groupName)) {
			final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter = new DistributionGroupZookeeperAdapter(zookeeperClient);
			final KDtreeZookeeperAdapter adapter = distributionGroupZookeeperAdapter.readDistributionGroup(groupName);
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
	 */
	public static synchronized KDtreeZookeeperAdapter getGroupForTableName(final String tableName, final ZookeeperClient zookeeperClient) throws ZookeeperException, BBoxDBException {
		final SSTableName ssTableName = new SSTableName(tableName);
		
		if(! ssTableName.isValid()) {
			throw new BBoxDBException("Invalid tablename: " + tableName);
		}
		
		return getGroupForGroupName(ssTableName.getDistributionGroup(), zookeeperClient);
	}
}
