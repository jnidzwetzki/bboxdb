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
package org.bboxdb.distribution.regionsplit;

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.StorageRegistry;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.FloatInterval;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableManager;

public class WeightBasedSplitStrategy extends RegionSplitStrategy {
	
	protected final int SAMPLE_SIZE;
	
	public WeightBasedSplitStrategy() {
		SAMPLE_SIZE = maxEntriesPerTable() / 100;
	}

	@Override
	protected void performSplit(final DistributionRegion region) {
		final int splitDimension = region.getSplitDimension();
		final List<SSTableName> tables = StorageRegistry.getAllTablesForDistributionGroup(region.getDistributionGroupName());
	
		try {
			final List<FloatInterval> floatIntervals = new ArrayList<FloatInterval>();

			for(final SSTableName ssTableName : tables) {
				logger.info("Create split samples for table: {} ", ssTableName.getFullname());
				
				final SSTableManager storageInterface = StorageRegistry.getSSTableManager(ssTableName);
				final List<ReadOnlyTupleStorage> facades = storageInterface.getTupleStoreInstances().getAllTupleStorages();
				
				processFacades(facades, splitDimension, region, floatIntervals);
				logger.info("Create split samples for table: {} DONE", ssTableName.getFullname());
			}
			
			final int midpoint = floatIntervals.size() / 2;
			final FloatInterval splitInterval = floatIntervals.get(midpoint);

			performSplitAtPosition(region, splitInterval.getBegin());
		} catch (StorageManagerException e) {
			logger.error("Got exception while performing split", e);
		}
	}

	/**
	 * Process the facades for the table and create samples
	 * @param storages
	 * @param splitDimension 
	 * @param region 
	 * @param floatIntervals 
	 * @throws StorageManagerException 
	 */
	protected void processFacades(final List<ReadOnlyTupleStorage> storages, final int splitDimension, DistributionRegion region, final List<FloatInterval> floatIntervals) throws StorageManagerException {
		
		final int samplesPerStorage = Math.max(10, SAMPLE_SIZE / storages.size());
		
		logger.debug("Using {} samples per storage", samplesPerStorage);
		
		for(final ReadOnlyTupleStorage storage : storages) {
			if(! storage.acquire() ) {
				continue;
			}
			
			final long numberOfTuples = storage.getNumberOfTuples();
			final int sampleOffset = Math.max(10, (int) (numberOfTuples / samplesPerStorage));
						
			for (int position = 0; position < numberOfTuples; position = position + sampleOffset) {
				final Tuple tuple = storage.getTupleAtPosition(position);							
				final BoundingBox tupleBoundingBox = tuple.getBoundingBox();
				
				// Only the in the region contained part of the tuple is relevant
				final BoundingBox groupBox = region.getConveringBox().getIntersection(tupleBoundingBox);
				
				final FloatInterval interval = groupBox.getIntervalForDimension(splitDimension);
				floatIntervals.add(interval);
			}
	
			storage.release();
		}
	}

}
