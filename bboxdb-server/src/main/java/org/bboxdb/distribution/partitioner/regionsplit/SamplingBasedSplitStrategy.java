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
package org.bboxdb.distribution.partitioner.regionsplit;

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DoubleInterval;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamplingBasedSplitStrategy implements SplitpointStrategy {
	
	/**
	 * The point samples
	 */
	private final List<Double> pointSamples;
	
	/**
	 * The disk storage
	 */
	private TupleStoreManagerRegistry tupleStoreManagerRegistry;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SamplingBasedSplitStrategy.class);


	public SamplingBasedSplitStrategy(final TupleStoreManagerRegistry tupleStoreManagerRegistry) {
		this.tupleStoreManagerRegistry = tupleStoreManagerRegistry;
		this.pointSamples = new ArrayList<>();
	}

	@Override
	public double getSplitPoint(final DistributionRegion region) throws StorageManagerException {
		
		final int splitDimension = region.getSplitDimension();
	
		final List<TupleStoreName> tables = tupleStoreManagerRegistry
				.getAllTablesForDistributionGroupAndRegionId
				(region.getDistributionGroupName(), region.getRegionId());
	
		return caclculateSplitPoint(region.getConveringBox(), splitDimension, tables);
	}

	/**
	 * Calculate the split point
	 * @param boundingBox
	 * @param splitDimension
	 * @param tables
	 * @return
	 * @throws StorageManagerException
	 */
	protected double caclculateSplitPoint(final BoundingBox boundingBox, final int splitDimension,
			final List<TupleStoreName> tables) throws StorageManagerException {
		
		// Get the samples
		getPointSamples(boundingBox, splitDimension, tables);
		
		// Sort points
		pointSamples.sort((i1, i2) -> Double.compare(i1,i2));
		
		// Calculate point
		final int midpoint = pointSamples.size() / 2;
		final double splitPosition = pointSamples.get(midpoint);
		final double splitPositonRound = MathUtil.round(splitPosition, 5);
		
		return splitPositonRound;
	}

	/**
	 * Get the begin and end point samples
	 * 
	 * @param boundingBox
	 * @param splitDimension
	 * @param tables
	 * @throws StorageManagerException
	 */
	protected void getPointSamples(final BoundingBox boundingBox, final int splitDimension,
			final List<TupleStoreName> tables) throws StorageManagerException {
		
		for(final TupleStoreName ssTableName : tables) {
			logger.info("Create split samples for table: {} ", ssTableName.getFullname());
			
			final TupleStoreManager sstableManager = tupleStoreManagerRegistry
					.getTupleStoreManager(ssTableName);
			
			final List<ReadOnlyTupleStore> tupleStores = sstableManager.getAllTupleStorages();
			processTupleStores(tupleStores, splitDimension, boundingBox);
			logger.info("Create split samples for table: {} DONE", ssTableName.getFullname());
		}
	}

	/**
	 * Process the facades for the table and create samples
	 * @param storages
	 * @param splitDimension 
	 * @param boundingBox 
	 * @param floatIntervals 
	 * @throws StorageManagerException 
	 */
	protected void processTupleStores(final List<ReadOnlyTupleStore> storages, final int splitDimension, 
			final BoundingBox boundingBox) throws StorageManagerException {
		
		final int samplesPerStorage = 100;
		
		logger.debug("Fetching {} samples per storage", samplesPerStorage);
		
		for(final ReadOnlyTupleStore storage : storages) {
			if(! storage.acquire() ) {
				continue;
			}
			
			final long numberOfTuples = storage.getNumberOfTuples();
			final int sampleOffset = Math.max(10, (int) (numberOfTuples / samplesPerStorage));
			
			for (int position = 0; position < numberOfTuples; position = position + sampleOffset) {
				final Tuple tuple = storage.getTupleAtPosition(position);							
				final BoundingBox tupleBoundingBox = tuple.getBoundingBox();
				
				// Ignore tuples with an empty box (e.g. deleted tuples)
				if(tupleBoundingBox.equals(BoundingBox.EMPTY_BOX)) {
					continue;
				}
				
				// Add the begin and end pos to the lists, if the begin / end is in the 
				// covering box
				final DoubleInterval tupleInterval = tupleBoundingBox.getIntervalForDimension(splitDimension);
				final DoubleInterval groupInterval = boundingBox.getIntervalForDimension(splitDimension);
				
				if(tupleInterval.getBegin() > groupInterval.getBegin()) {
					pointSamples.add(tupleInterval.getBegin());
				}
				
				if(tupleInterval.getEnd() < groupInterval.getEnd()) {
					pointSamples.add(tupleInterval.getEnd());
				}
			}
	
			storage.release();
		}
	}
}
