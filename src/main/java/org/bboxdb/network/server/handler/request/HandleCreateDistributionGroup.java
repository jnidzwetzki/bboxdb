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
package org.bboxdb.network.server.handler.request;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.bboxdb.distribution.DistributionGroupCache;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.mode.DistributionRegionState;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.CreateDistributionGroupRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.server.ClientConnectionHandler;
import org.bboxdb.network.server.ErrorMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandleCreateDistributionGroup implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(HandleCreateDistributionGroup.class);
	

	@Override
	/**
	 * Create a new distribution group
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) throws IOException, PackageEncodeException {
		
		if(logger.isDebugEnabled()) {
			logger.debug("Got create distribution group package");
		}
		
		try {
			final CreateDistributionGroupRequest createPackage = CreateDistributionGroupRequest.decodeTuple(encodedPackage);
			logger.info("Create distribution group: " + createPackage.getDistributionGroup());
			
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
			
			distributionGroupZookeeperAdapter.createDistributionGroup(createPackage.getDistributionGroup(), createPackage.getReplicationFactor());
			
			final KDtreeZookeeperAdapter distributionAdapter = DistributionGroupCache.getGroupForGroupName(
					createPackage.getDistributionGroup(), zookeeperClient);

			final DistributionRegion region = distributionAdapter.getAndWaitForRootNode();
			
			distributionAdapter.allocateSystemsToNewRegion(region);
			
			// Let the data settle down
			Thread.sleep(5000);
			
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(region, DistributionRegionState.ACTIVE);
			
			clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
		} catch (Exception e) {
			logger.warn("Error while create distribution group", e);
			
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(responsePackage);
		}
		
		return true;
	}
}
