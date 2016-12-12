/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.distribution.mode;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import de.fernunihagen.dna.scalephant.distribution.DistributionRegion;

public class KDtreeZookeeperAdapter implements Watcher {
		
	/**
	 * The distribution group adapter
	 */
	protected final DistributionGroupZookeeperAdapter distributionGroupAdapter;
	
	/**
	 * The root node of the K-D-Tree
	 */
	protected final DistributionRegion rootNode;

	public KDtreeZookeeperAdapter(final DistributionGroupZookeeperAdapter distributionGroupAdapter, final DistributionRegion rootNode) {
		this.distributionGroupAdapter = distributionGroupAdapter;
		this.rootNode = rootNode;
	}
	
	/**
	 * Get the root node
	 * @return
	 */
	public DistributionRegion getRootNode() {
		return rootNode;
	}

	@Override
	public void process(final WatchedEvent event) {

	}

}
