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

import java.nio.ByteBuffer;

import org.apache.zookeeper.Watcher;
import org.bboxdb.commons.io.DataEncoderHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeMutationHelper {
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(NodeMutationHelper.class);

	/**
	 * Update the version for the node
	 * @param path
	 * @param position
	 * @throws ZookeeperException 
	 */
	public static void markNodeMutationAsComplete(final ZookeeperClient zookeeperClient, 
			final String path) throws ZookeeperException {
		
		final ByteBuffer versionBytes = DataEncoderHelper.longToByteBuffer(System.currentTimeMillis());
		
		final String nodePath = path + "/" + ZookeeperNodeNames.NAME_NODE_VERSION;
		
		logger.debug("Mark mutation as complete {}", path);
		
		zookeeperClient.replacePersistentNode(nodePath, versionBytes.array());
	}
	
	/**
	 * Is the node completely created?
	 * @param path
	 * @return
	 * @throws ZookeeperException 
	 */
	public static boolean isNodeCompletelyCreated(final ZookeeperClient zookeeperClient, 
			final String path) throws ZookeeperException {
		
		return zookeeperClient.exists(path + "/" + ZookeeperNodeNames.NAME_NODE_VERSION);
	}
	
	/**
	 * Get the version of the node mutation
	 * @param path
	 * @return
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	public static long getNodeMutationVersion(final ZookeeperClient zookeeperClient,
			final String path, final Watcher watcher) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		logger.debug("Reading mutation from path {}", path);
		
		final byte[] result = zookeeperClient.readPathAndReturnBytes(
				path + "/" + ZookeeperNodeNames.NAME_NODE_VERSION, watcher);
		
		return DataEncoderHelper.readLongFromByte(result);
	}
	
}
