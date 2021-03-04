/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.networkproxy.handler;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.BBoxDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoinLocalHandler extends AbstractJoinHandler {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(JoinLocalHandler.class);

	@Override
	protected void executeLogging(String table1, String table2, Hyperrectangle bbox) {
		logger.info("Got get local join for table1 {} / table2 {} and bbox {}", table1, table2, bbox);		
	}

	@Override
	public BBoxDB getConnection(BBoxDBCluster bboxdbClient) {
		final MembershipConnectionService connectionService = MembershipConnectionService.getInstance();
		final BBoxDBConnection localConnection = connectionService.getConnectionForInstance(ZookeeperClientFactory.getLocalInstanceName());
		return localConnection.getBboxDBClient();
	}
}
