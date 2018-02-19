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
package org.bboxdb.distribution;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.bboxdb.misc.Const;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributionRegionIdMapper {

	/**
	 * The mappings
	 */
	protected final List<RegionTablenameEntry> regions = new CopyOnWriteArrayList<RegionTablenameEntry>();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionRegionIdMapper.class);
	
	/**
	 * Search the region ids that are overlapped by the bounding box
	 */
	public Set<Long> getRegionIdsForRegion(final BoundingBox region) {
		return regions
			.stream()
			.filter(r -> r.getBoundingBox().overlaps(region))
			.map(r -> r.getRegionId())
			.collect(Collectors.toSet());
	}
	
	/**
	 * Get all region ids
	 * @return
	 */
	public Set<Long> getAllRegionIds() {
		return regions
			.stream()
			.map(r -> r.getRegionId())
			.collect(Collectors.toSet());
	}
	
	/**
	 * Get the SSTables that are responsible for a given bounding box
	 * @param region
	 * @param ssTableName
	 * @return
	 */
	public Collection<TupleStoreName> getLocalTablesForRegion(final BoundingBox region, 
			final TupleStoreName ssTableName) {
	
		Collection<Long> namprefixes = null;
		
		for(int execution = 0; execution < Const.OPERATION_RETRY; execution++) {
			namprefixes = getRegionIdsForRegion(region);
			
			if(! namprefixes.isEmpty()) {
				break;
			}
			
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		
		if(namprefixes.isEmpty() && logger.isDebugEnabled()) {
			logger.debug("Got an empty result list by query region: {}", region);
		}
		
		return convertRegionIdToTableNames(ssTableName, namprefixes);
	}
	
	/**
	 * Get all SSTables that are stored local
	 * @param ssTableName
	 * @return
	 */
	public List<TupleStoreName> getAllLocalTables(final TupleStoreName ssTableName) {
		final Collection<Long> namprefixes = getAllRegionIds();
		
		if(namprefixes.isEmpty() && logger.isWarnEnabled()) {
			logger.warn("Got an empty result list by query all regions");
		}
		
		return convertRegionIdToTableNames(ssTableName, namprefixes);		
	}

	/**
	 * Prefix all entries of the given list with the name of the sstable
	 * @param ssTableName
	 * @param regionIds
	 * @return
	 */
	public List<TupleStoreName> convertRegionIdToTableNames(final TupleStoreName ssTableName,
			final Collection<Long> regionIds) {
		
		return regionIds
				.stream()
				.map(i -> ssTableName.cloneWithDifferntRegionId(i))
				.collect(Collectors.toList());
	}
	
	/**
	 * Add a new mapping
	 * @param tablename
	 * @param boundingBox
	 */
	public boolean addMapping(final DistributionRegion region) {
				
		final long regionId = region.getRegionId();
		final BoundingBox converingBox = region.getConveringBox();	
		
		final boolean known = regions.stream().anyMatch(r -> r.getRegionId() == regionId);
		
		if(known) {
			logger.debug("Mapping for region {} already exists, ignoring", regionId);
			return false;
		}
		
		logger.info("Add local mapping for: {}", region.getIdentifier());
		regions.add(new RegionTablenameEntry(converingBox, regionId));
		
		return true;
	}
	
	/**
	 * Remove a mapping
	 * @return
	 */
	public boolean removeMapping(final long regionId) {
		// Removal is supported by COW array list
		final boolean removed = regions.removeIf(r -> r.getRegionId() == regionId);
		
		if(removed) {
			logger.info("Mapping for region id {} removed", regionId);
		}
		
		return removed;
	}
	
	/**
	 * Remove all mappings
	 */
	public void clear() {
		logger.info("Clear all local mappings");
		regions.clear();
	}
}

class RegionTablenameEntry {
	protected final BoundingBox boundingBox;
	protected final long regionId;
	
	public RegionTablenameEntry(final BoundingBox boundingBox, final long regionId) {
		this.boundingBox = boundingBox;
		this.regionId = regionId;
	}

	public BoundingBox getBoundingBox() {
		return boundingBox;
	}

	public long getRegionId() {
		return regionId;
	}
	
	@Override
	public String toString() {
		return "RegionTablenameEntry [boundingBox=" + boundingBox + ", regionId=" + regionId + "]";
	}
}