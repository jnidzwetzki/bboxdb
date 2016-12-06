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
package de.fernunihagen.dna.scalephant.distribution.regionsplit;

import java.util.ArrayList;
import java.util.List;

import de.fernunihagen.dna.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.scalephant.storage.ReadOnlyTupleStorage;
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.StorageRegistry;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.FloatInterval;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;

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
				
				processFacades(facades, splitDimension, floatIntervals);
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
	 * @param floatIntervals 
	 * @throws StorageManagerException 
	 */
	protected void processFacades(final List<ReadOnlyTupleStorage> storages, final int splitDimension, final List<FloatInterval> floatIntervals) throws StorageManagerException {
		
		final int samplesPerFacade = Math.max(10, SAMPLE_SIZE / storages.size());
		
		for(final ReadOnlyTupleStorage storage : storages) {
			if(! storage.acquire() ) {
				continue;
			}
			
			final long numberOfTuples = storage.getNumberOfTuples();
			final int sampleOffset = (int) (numberOfTuples / samplesPerFacade);
			
			for (int position = 0; position < numberOfTuples; position = position + sampleOffset) {
				final Tuple tuple = storage.getTupleAtPosition(position);							
				final BoundingBox boundingBox = tuple.getBoundingBox();
				final FloatInterval interval = boundingBox.getIntervalForDimension(splitDimension);
				floatIntervals.add(interval);
			}
	
			storage.release();
		}
	}

}
