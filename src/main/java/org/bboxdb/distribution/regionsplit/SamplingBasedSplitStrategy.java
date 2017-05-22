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
package org.bboxdb.distribution.regionsplit;

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.StorageRegistry;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DoubleInterval;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableManager;

public class SamplingBasedSplitStrategy extends AbstractRegionSplitStrategy {
	
	@Override
	protected boolean performSplit(final DistributionRegion regionToSplit) {
		final int splitDimension = regionToSplit.getSplitDimension();
	
		final List<SSTableName> tables = StorageRegistry.getInstance()
				.getAllTablesForDistributionGroupAndRegionId
				(region.getDistributionGroupName(), region.getRegionId());
		
		try {
			final List<DoubleInterval> doubleIntervals = new ArrayList<DoubleInterval>();

			for(final SSTableName ssTableName : tables) {
				logger.info("Create split samples for table: {} ", ssTableName.getFullname());
				
				final SSTableManager storageInterface = StorageRegistry.getInstance().getSSTableManager(ssTableName);
				final List<ReadOnlyTupleStorage> tupleStores = storageInterface.getTupleStoreInstances().getAllTupleStorages();
				
				processTupleStores(tupleStores, splitDimension, regionToSplit, doubleIntervals);
				logger.info("Create split samples for table: {} DONE", ssTableName.getFullname());
			}
			
			// Sort intervals and take the middle element as split interval
			doubleIntervals.sort((i1, i2) -> Double.compare(i1.getBegin(),i2.getBegin()));
			final int midpoint = doubleIntervals.size() / 2;
			final DoubleInterval splitInterval = doubleIntervals.get(midpoint);

			final double splitPosition = splitInterval.getBegin();
			final double splitPositonRound = round(splitPosition, 7);
			
			performSplitAtPosition(regionToSplit, splitPositonRound);
			
			return true;
		} catch (StorageManagerException e) {
			logger.error("Got exception while performing split", e);
			return false;
		}
	}
	
	/**
	 * Round the given number of fractions
	 * @param value
	 * @param frac
	 * @return
	 */
    protected double round(final double value, final int frac) {
        final double pow = Math.pow(10.0, frac);
        
		return Math.round(pow * value) / pow;
    }

	/**
	 * Process the facades for the table and create samples
	 * @param storages
	 * @param splitDimension 
	 * @param regionToSplit 
	 * @param floatIntervals 
	 * @throws StorageManagerException 
	 */
	protected void processTupleStores(final List<ReadOnlyTupleStorage> storages, final int splitDimension, 
			final DistributionRegion regionToSplit, final List<DoubleInterval> floatIntervals) 
					throws StorageManagerException {
		
		final int samplesPerStorage = 100;
		
		logger.debug("Fetching {} samples per storage", samplesPerStorage);
		
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
				final BoundingBox groupBox = regionToSplit.getConveringBox().getIntersection(tupleBoundingBox);
				
				// Ignore tuples with an empty box (e.g. deleted tuples)
				if(groupBox.equals(BoundingBox.EMPTY_BOX)) {
					continue;
				}
					
				final DoubleInterval interval = groupBox.getIntervalForDimension(splitDimension);
				floatIntervals.add(interval);
			}
	
			storage.release();
		}
	}

}
