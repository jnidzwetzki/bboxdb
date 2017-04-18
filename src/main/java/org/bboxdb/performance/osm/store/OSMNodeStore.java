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
package org.bboxdb.performance.osm.store;

import java.sql.SQLException;

import org.bboxdb.performance.osm.util.SerializableNode;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;

public interface OSMNodeStore {

	/**
	 * Close all resources
	 */
	void close();

	/**
	 * Store a new node
	 * 
	 * @param node
	 * @throws Exception 
	 */
	void storeNode(Node node) throws Exception;

	/**
	 * Get the id for the node
	 * 
	 * @param nodeId
	 * @return
	 * @throws SQLException
	 */
	SerializableNode getNodeForId(long nodeId) throws SQLException;

}