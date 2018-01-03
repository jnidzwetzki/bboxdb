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
package org.bboxdb.storage.sstable.spatialindex.rtree;

import java.util.concurrent.atomic.AtomicInteger;

public class RTreeNodeFactory {

	/**
	 * The node id generator
	 */
	protected final AtomicInteger nodeIdInteger;

	public RTreeNodeFactory() {
		this.nodeIdInteger = new AtomicInteger(0);
	}
	
	/**
	 * Build a new directory node
	 * @return 
	 */
	public RTreeDirectoryNode buildDirectoryNode() {
		return new RTreeDirectoryNode(getNextNodeId());
	}
	
	/**
	 * Get the next available node id
	 * @return
	 */
	protected int getNextNodeId() {
		return nodeIdInteger.getAndIncrement();
	}

}
