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
package org.bboxdb.distribution.membership;

import org.bboxdb.distribution.zookeeper.ZookeeperClient;

public class ZookeeperInstancePathHelper {

	/**
	 * The zookeeper client
	 */
	private final ZookeeperClient zookeeperClient;

	public ZookeeperInstancePathHelper(final ZookeeperClient zookeeperClient) {
		this.zookeeperClient = zookeeperClient;
	}

	/**
	 * Get the node info path
	 * @param distributedInstance 
	 * @return
	 */
	public String getInstanceDetailsPath(final BBoxDBInstance distributedInstance) {
		return zookeeperClient.getDetailsPath() + "/" + distributedInstance.getStringValue();
	}
	
	/**
	 * Get the path of the version node
	 */
	public String getInstancesVersionPath(final BBoxDBInstance distributedInstance) {
		return getInstanceDetailsPath(distributedInstance) + "/version";
	}
	

	/**
	 * Get the path of the cpu core node
	 */
	public String getInstancesCpuCorePath(final BBoxDBInstance distributedInstance) {
		return getInstanceDetailsPath(distributedInstance) + "/cpucore";
	}
	
	/**
	 * Get the path of the memory node
	 */
	public String getInstancesMemoryPath(final BBoxDBInstance distributedInstance) {
		return getInstanceDetailsPath(distributedInstance) + "/memory";
	}
	
	/**
	 * Get the path of the diskspace node
	 */
	public String getInstancesDiskspacePath(final BBoxDBInstance distributedInstance) {
		return getInstanceDetailsPath(distributedInstance) + "/diskspace";
	}

	/**
	 * Get the free space of the diskspace node
	 */
	public String getInstancesDiskspaceFreePath(final BBoxDBInstance distributedInstance, 
			final String path) {
		final String zookeeperPath = quotePath(path);
		return getInstancesDiskspacePath(distributedInstance) + "/" + zookeeperPath + "/free";
	}
	
	/**
	 * Get the total space of the diskspace node
	 */
	public String getInstancesDiskspaceTotalPath(final BBoxDBInstance distributedInstance, 
			final String path) {
		final String zookeeperPath = quotePath(path);
		return getInstancesDiskspacePath(distributedInstance) + "/" + zookeeperPath + "/total";
	}
	

	/**
	 * Quote the file system path (replace all '/' with '__') to get 
	 * a valid zookeeper node name
	 * 
	 * @param path
	 * @return
	 */
	public static String quotePath(final String path) {
		return path.replaceAll("/", "__");
	}
	
	/**
	 * Unquote the given path
	 * @param path
	 * @return
	 */
	public static String unquotePath(final String path) {
		return path.replaceAll("__", "/");
	}
	
}
