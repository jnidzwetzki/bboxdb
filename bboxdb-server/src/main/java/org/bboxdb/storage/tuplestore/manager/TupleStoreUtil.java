/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb.storage.tuplestore.manager;

import java.util.List;
import java.util.stream.Collectors;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.TupleStoreName;

public class TupleStoreUtil {
	
	/**
	 * Get all tables for the given distribution group and region id
	 * @param distributionGroupName 
	 * @param regionId
	 * @return
	 */
	public static List<TupleStoreName> getAllTablesForDistributionGroupAndRegionId(
			final TupleStoreManagerRegistry registry, final String distributionGroupName, 
			final long regionId) {
		
		final List<TupleStoreName> groupTables = registry.getAllTablesForDistributionGroup(
				distributionGroupName);
		
		return groupTables
			.stream()
			.filter(s -> s.getRegionId().getAsLong() == regionId)
			.collect(Collectors.toList());
	}
	
	/**
	 * Set all tables in the region to read only state
	 * @param registry
	 * @param distributionGroupName
	 * @param regionId
	 * @throws StorageManagerException 
	 */
	public static void setAllTablesToReadOnly(final TupleStoreManagerRegistry registry, 
			final String distributionGroupName, 
			final long regionId) throws StorageManagerException {
		
		final List<TupleStoreName> localTables 
			= getAllTablesForDistributionGroupAndRegionId(registry, distributionGroupName, regionId);
		
		for(final TupleStoreName ssTableName : localTables) {
			final TupleStoreManager ssTableManager = registry.getTupleStoreManager(ssTableName);			
			ssTableManager.setToReadOnly();
		}
		
	}
	
	/**
	 * Set all tables in the region to read write state
	 * @param registry
	 * @param distributionGroupName
	 * @param regionId
	 * @throws StorageManagerException 
	 */
	public static void setAllTablesToReadWrite(final TupleStoreManagerRegistry registry, 
			final String distributionGroupName, 
			final long regionId) throws StorageManagerException {
		
		final List<TupleStoreName> localTables 
		= getAllTablesForDistributionGroupAndRegionId(registry, distributionGroupName, regionId);
	
		for(final TupleStoreName ssTableName : localTables) {
			final TupleStoreManager ssTableManager = registry.getTupleStoreManager(ssTableName);			
			ssTableManager.setToReadWrite();
		}
		
	}
	

	/**
	 * Get the size of all sstables in the distribution group and region id
	 * @param distributionGroupName
	 * @param regionId
	 * @return
	 * @throws StorageManagerException
	 */
	public static long getSizeOfDistributionGroupAndRegionId(final TupleStoreManagerRegistry registry, 
			final String distributionGroupName, final long regionId) 
				throws StorageManagerException {
		
		final List<TupleStoreName> tables 
			= getAllTablesForDistributionGroupAndRegionId(registry, distributionGroupName, regionId);
		
		long totalSize = 0;
		
		for(final TupleStoreName ssTableName : tables) {
			final TupleStoreManager tupleStoreManager = registry.getTupleStoreManager(ssTableName);
			totalSize = totalSize + tupleStoreManager.getSize();
		}
		
		return totalSize;
	}
	
	/**
	 * Get the amount of tuples  in the distribution group and region id
	 * @param distributionGroupName
	 * @param regionId
	 * @return
	 * @throws StorageManagerException
	 */
	public static long getTuplesInDistributionGroupAndRegionId(final TupleStoreManagerRegistry registry, 
			final String distributionGroupName, final long regionId) 
				throws StorageManagerException {
		
		final List<TupleStoreName> tables 
			= getAllTablesForDistributionGroupAndRegionId(registry, distributionGroupName, regionId);
		
		long tuples = 0;
		
		for(final TupleStoreName ssTableName : tables) {
			final TupleStoreManager tupleStoreManager = registry.getTupleStoreManager(ssTableName);
			tuples = tuples + tupleStoreManager.getNumberOfTuples();
		}
		
		return tuples;
	}
	
}
