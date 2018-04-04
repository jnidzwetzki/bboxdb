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

import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.DeleteDistributionGroupRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.server.ErrorMessages;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteDistributionGroupHandler implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DeleteDistributionGroupHandler.class);
	

	@Override
	/**
	 * Delete an existing distribution group
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) throws IOException, PackageEncodeException {

		try {
			final DeleteDistributionGroupRequest deletePackage = DeleteDistributionGroupRequest.decodeTuple(encodedPackage);
			final String distributionGroup = deletePackage.getDistributionGroup();
			
			logger.info("Delete distribution group: {}", distributionGroup);
			
			// Delete in Zookeeper
			final DistributionGroupAdapter distributionGroupZookeeperAdapter 
				= ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
			distributionGroupZookeeperAdapter.deleteDistributionGroup(distributionGroup);

			// Delete local stored data
			logger.info("Delete distribution group, delete local stored data");
			clientConnectionHandler.getStorageRegistry().deleteAllTablesInDistributionGroup(distributionGroup);
			
			// Clear cached data
			TupleStoreConfigurationCache.getInstance().clear();
			
			logger.info("Delete distribution group - DONE");
			clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
		} catch (Exception e) {
			logger.warn("Error while delete distribution group", e);
			
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(responsePackage);
		}
		
		return true;
	}
}
