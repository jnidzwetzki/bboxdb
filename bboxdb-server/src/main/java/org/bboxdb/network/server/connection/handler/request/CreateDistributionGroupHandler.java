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
package org.bboxdb.network.server.connection.handler.request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.DistributionRegionAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.CreateDistributionGroupRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.server.ErrorMessages;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDistributionGroupHandler implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(CreateDistributionGroupHandler.class);
	

	@Override
	/**
	 * Create a new distribution group
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) throws IOException, PackageEncodeException {

		try {
			final CreateDistributionGroupRequest createPackage = CreateDistributionGroupRequest.decodeTuple(encodedPackage);
			final String distributionGroup = createPackage.getDistributionGroup();
			logger.info("Create distribution group: {}", distributionGroup);
			
			final DistributionGroupAdapter distributionGroupAdapter 
				= ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
			
			final DistributionRegionAdapter distributionRegionAdapter 
				= ZookeeperClientFactory.getZookeeperClient().getDistributionRegionAdapter();
			
			final List<String> knownGroups = distributionGroupAdapter.getDistributionGroups();
			if(knownGroups.contains(distributionGroup)) {
				logger.error("Untable to create distributon group {}, already exists", distributionGroup);
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_DGROUP_EXISTS);
				clientConnectionHandler.writeResultPackage(responsePackage);
				return true;
			}
			
			distributionGroupAdapter.createDistributionGroup(distributionGroup, 
					createPackage.getDistributionGroupConfiguration());
			
			final SpacePartitioner spacePartitioner = SpacePartitionerCache.getInstance()
					.getSpacePartitionerForGroupName(distributionGroup);

			final DistributionRegion region = spacePartitioner.getRootNode();
			
			distributionRegionAdapter.setStateForDistributionRegion(region, DistributionRegionState.ACTIVE);
			
			clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
		} catch (Exception e) {
			logger.warn("Error while create distribution group", e);
			
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(responsePackage);
		}
		
		return true;
	}
}
