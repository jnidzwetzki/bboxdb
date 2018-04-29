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
import java.util.Collection;
import java.util.List;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.bboxdb.storage.tuplestore.manager.TupleStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamplingHelper {

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SamplingHelper.class);
	
	/**
	 * Create the samples
	 * @param region
	 * @param tupleStoreManagerRegistry
	 * @return
	 * @throws StorageManagerException 
	 */
	public static Collection<Hyperrectangle> getSamplesForRegion(DistributionRegion region,
			TupleStoreManagerRegistry tupleStoreManagerRegistry) throws StorageManagerException {
	
		final List<TupleStoreName> tables = TupleStoreUtil
				.getAllTablesForDistributionGroupAndRegionId
				(tupleStoreManagerRegistry, region.getDistributionGroupName(), region.getRegionId());
	
		return getSamples(region.getConveringBox(), tupleStoreManagerRegistry, tables);
	}

	/**
	 * Get the samples
	 * 
	 * @param boundingBox
	 * @param splitDimension
	 * @param tables
	 * @return 
	 * @throws StorageManagerException
	 */
	private static List<Hyperrectangle> getSamples(final Hyperrectangle boundingBox, 
			final TupleStoreManagerRegistry tupleStoreManagerRegistry,
			final List<TupleStoreName> tables) throws StorageManagerException {
		
		final List<Hyperrectangle> allPointSamples = new ArrayList<>();
		
		for(final TupleStoreName ssTableName : tables) {
			logger.info("Create split samples for table: {} ", ssTableName.getFullname());
			
			final TupleStoreManager sstableManager = tupleStoreManagerRegistry
					.getTupleStoreManager(ssTableName);
			
			final List<ReadOnlyTupleStore> tupleStores = sstableManager.getAllTupleStorages();
			
			final List<Hyperrectangle> pointSamples = processTupleStores(tupleStores);
			allPointSamples.addAll(pointSamples);
			
			logger.info("Create split samples for table: {} DONE. Got {} samples.", 
					ssTableName.getFullname(), pointSamples.size());
		}
				
		return allPointSamples;
	}

	/**
	 * Process the facades for the table and create samples
	 * @param storages
	 * @param splitDimension 
	 * @param boundingBox 
	 * @param floatIntervals 
	 * @return 
	 * @throws StorageManagerException 
	 */
	private static List<Hyperrectangle> processTupleStores(final List<ReadOnlyTupleStore> storages) 
			throws StorageManagerException {
		
		final int samplesPerStorage = 100;
		final List<Hyperrectangle> samples = new ArrayList<>();
		
		logger.debug("Fetching {} samples per storage", samplesPerStorage);
		
		for(final ReadOnlyTupleStore storage : storages) {
			if(! storage.acquire() ) {
				continue;
			}
			
			final long numberOfTuples = storage.getNumberOfTuples();
			final int sampleOffset = Math.max(10, (int) (numberOfTuples / samplesPerStorage));
			
			for (long position = 0; position < numberOfTuples; position = position + sampleOffset) {
				final Tuple tuple = storage.getTupleAtPosition(position);							
				final Hyperrectangle tupleBoundingBox = tuple.getBoundingBox();
			
				// Ignore tuples with an empty box (e.g. deleted tuples)
				if(tupleBoundingBox == null || tupleBoundingBox.equals(Hyperrectangle.FULL_SPACE)) {
					continue;
				}
				
				samples.add(tupleBoundingBox);
			}
	
			storage.release();
		}
		
		return samples;
	}
}
