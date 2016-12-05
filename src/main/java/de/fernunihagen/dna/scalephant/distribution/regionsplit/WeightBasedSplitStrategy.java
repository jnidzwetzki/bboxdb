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
import de.fernunihagen.dna.scalephant.storage.StorageRegistry;
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.FloatInterval;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableFacade;

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
				logger.info("Create split samples for table: " + ssTableName.getFullname());
				
				final SSTableManager storageInterface = StorageRegistry.getSSTableManager(ssTableName);
				final List<SSTableFacade> facades = storageInterface.getSstableFacades();
				
				processFacades(facades, splitDimension, floatIntervals);
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
	 * @param facades
	 * @param splitDimension 
	 * @param floatIntervals 
	 */
	protected void processFacades(final List<SSTableFacade> facades, final int splitDimension, final List<FloatInterval> floatIntervals) {
		
		final int samplesPerFacade = Math.max(10, SAMPLE_SIZE / facades.size());
		
		for(final SSTableFacade facade : facades) {
			if(! facade.acquire() ) {
				continue;
			}
			
			final long numberOfTuples = facade.getSsTableMetadata().getTuples();
			final int sampleOffset = (int) (numberOfTuples / samplesPerFacade);
			
			try {
				for (int position = 0; position < numberOfTuples; position = position + sampleOffset) {
					final Tuple tuple = facade.getSsTableReader().getTupleAtPosition(position);
					final BoundingBox boundingBox = tuple.getBoundingBox();
					final FloatInterval interval = boundingBox.getIntervalForDimension(splitDimension);
					floatIntervals.add(interval);
				}
			} catch (StorageManagerException e) {
				logger.error("Got an exception while reading sample tuples", e);
			}
	
			facade.release();
		}
	}

}
