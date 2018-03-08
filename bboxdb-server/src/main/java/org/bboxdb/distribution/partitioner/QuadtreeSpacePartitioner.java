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
package org.bboxdb.distribution.partitioner;

import java.util.Collection;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.misc.BBoxDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuadtreeSpacePartitioner extends AbstractTreeSpacePartitoner {

	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(QuadtreeSpacePartitioner.class);


	@Override
	public boolean isMergingSupported(final DistributionRegion distributionRegion) {
		return ! distributionRegion.isLeafRegion();
	}
	
	@Override
	public boolean isSplittingSupported(final DistributionRegion distributionRegion) {
		return distributionRegion.isLeafRegion();
	}

	@Override
	public void splitRegion(final DistributionRegion regionToSplit, 
			final Collection<BoundingBox> samples) throws BBoxDBException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void splitComplete(final DistributionRegion regionToSplit) throws BBoxDBException {
		// TODO Auto-generated method stub
		
	}

	
	@Override
	public void splitFailed(final DistributionRegion regionToSplit) throws BBoxDBException {
		// TODO Auto-generated method stub
		
	}
	

	@Override
	public void prepareMerge(final DistributionRegion regionToMerge) throws BBoxDBException {
		// TODO Auto-generated method stub

	}
	
	@Override
	public void mergeComplete(final DistributionRegion regionToMerge) throws BBoxDBException {
		// TODO Auto-generated method stub

	}
	
	@Override
	public void mergeFailed(final DistributionRegion regionToMerge) throws BBoxDBException {
		// TODO Auto-generated method stub
		
	}
}
