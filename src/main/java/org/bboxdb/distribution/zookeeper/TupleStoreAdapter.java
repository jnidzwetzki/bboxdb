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
package org.bboxdb.distribution.zookeeper;

import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;

public class TupleStoreAdapter {

	protected final ZookeeperClient zookeeperClient;

	public TupleStoreAdapter(final ZookeeperClient zookeeperClient) {
		this.zookeeperClient = zookeeperClient;
	}
	
	/**
	 * Write the tuple store configuration
	 * @param tupleStoreName
	 * @param tupleStoreConfiguration
	 */
	public void writeTuplestoreConfiguration(final TupleStoreName tupleStoreName, 
			final TupleStoreConfiguration tupleStoreConfiguration) {
		
	}
	
	/**
	 * Read the tuple store name
	 * @param tupleStoreName
	 */
	public TupleStoreConfiguration readTuplestoreConfiguration(final TupleStoreName tupleStoreName) {
		// FIXME:
		return null;
	}
	

}
