/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.bboxdb.commons.Retryer;
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
	 * @return 
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 */
	public static long markNodeMutationAsComplete(final ZookeeperClient zookeeperClient, 
			final String path) throws ZookeeperException {

		final Callable<Long> versionUpdate = () -> {
			// Ensure we don't set an older version
			final Stat stat = new Stat();
			long oldVersion = 0;
					
			try {
				oldVersion = getNodeMutationVersion(zookeeperClient, path, null, stat);
			} catch (ZookeeperNotFoundException e) {
				// ignore not found
			}
			
			// Use logical time to prevent time synchronization issues
			long newVersion = oldVersion + 1;
			
			final ByteBuffer versionBytes = DataEncoderHelper.longToByteBuffer(newVersion);
			
			final String nodePath = path + "/" + ZookeeperNodeNames.NAME_NODE_VERSION;
			
			zookeeperClient.replacePersistentNode(nodePath, versionBytes.array(), stat.getVersion());
			
			logger.debug("Mark mutation as complete {} (old={}, new={})", path, oldVersion, newVersion);
			
			return newVersion;
		};
		
		// Retry version update (might fail if two updates are performed in parallel)
		final Retryer<Long> versionUpdateRetryer = new Retryer<>(25, 10, TimeUnit.MILLISECONDS, versionUpdate);
		
		try {
			if(versionUpdateRetryer.execute()) {
				throw new ZookeeperException(versionUpdateRetryer.getLastException());
			}
		} catch (InterruptedException e) {
			throw new ZookeeperException(e);
		} 
		
		return versionUpdateRetryer.getResult();
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
			final String path, final Watcher watcher, final Stat stat) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		logger.debug("Reading mutation from path {}", path);
		
		final byte[] result = zookeeperClient.readPathAndReturnBytes(
				path + "/" + ZookeeperNodeNames.NAME_NODE_VERSION, watcher, stat);
		
		return DataEncoderHelper.readLongFromByte(result);
	}
	
}
