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
package de.fernunihagen.dna.scalephant.storage;

import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;

public class BalancedIntervalSearchTree {

	/**
	 * The dimension of the tree
	 */
	protected final int dimension;

	/**
	 * The root element of the tree
	 */
	protected BalancedIntervalSearchTreeNode root = null;
	
	public BalancedIntervalSearchTree(final int dimension) {
		super();
		this.dimension = dimension;
	}
	
	/**
	 * Insert a new 
	 * @param boundingBox
	 * @return
	 */
	public boolean insert(final BoundingBox boundingBox) {
		
		if(boundingBox.getDimension() != dimension) {
			return false;
		}
		
		final BalancedIntervalSearchTreeNode newNode = new BalancedIntervalSearchTreeNode(boundingBox);
		
		if(root == null) {
			root = newNode;
		}
		
		return true;
	}
	
}

class BalancedIntervalSearchTreeNode {
	protected final BoundingBox boundingBox;
	
	public BalancedIntervalSearchTreeNode(final BoundingBox boundingBox) {
		this.boundingBox = boundingBox;
	}
}