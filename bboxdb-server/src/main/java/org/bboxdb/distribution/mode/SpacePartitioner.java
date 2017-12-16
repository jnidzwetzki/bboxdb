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
package org.bboxdb.distribution.mode;

import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;

public interface SpacePartitioner {
	
	/**
	 * All dependencies are set, init the partitioner
	 * @throws ZookeeperException
	 */
	public void init() throws ZookeeperException;

	/**
	 * Inject the distribution group
	 * @param distributionGroupName
	 */
	public void setDistributionGroup(final DistributionGroupName distributionGroupName);

	/**
	 * Inject the zookeeper client
	 * @param zookeeperClient
	 */
	public void setZookeeperClient(final ZookeeperClient zookeeperClient);

	/**
	 * Inject the distribution group adapter
	 * @param distributionGroupAdapter
	 */
	public void setDistributionGroupAdapter(final DistributionGroupZookeeperAdapter distributionGroupAdapter);
}
