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
package org.bboxdb.distribution;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionIdMapper {

	/**
	 * The mappings
	 */
	protected final List<RegionTablenameEntry> regions = new CopyOnWriteArrayList<RegionTablenameEntry>();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RegionIdMapper.class);
	
	/**
	 * Search the region ids that are overlapped by the bounding box
	 */
	public Collection<Integer> getRegionIdsForRegion(final BoundingBox region) {
		return regions
			.stream()
			.filter(r -> r.getBoundingBox().overlaps(region))
			.map(r -> r.getRegionId())
			.collect(Collectors.toList());
	}
	
	/**
	 * Get all region ids
	 * @return
	 */
	public Collection<Integer> getAllRegionIds() {
		return regions
			.stream()
			.map(r -> r.getRegionId())
			.collect(Collectors.toList());
	}
	
	/**
	 * Get the SSTables that are responsible for a given bounding box
	 * @param region
	 * @param ssTableName
	 * @return
	 */
	public Collection<SSTableName> getLocalTablesForRegion(final BoundingBox region, 
			final SSTableName ssTableName) {
		
		final Collection<Integer> namprefixes = getRegionIdsForRegion(region);
		
		if(namprefixes.isEmpty() && logger.isWarnEnabled()) {
			logger.warn("Got an empty result list by query region: " + region);
		}
		
		return convertRegionIdToTableNames(ssTableName, namprefixes);
	}
	
	
	/**
	 * Get all SSTables that are stored local
	 * @param ssTableName
	 * @return
	 */
	public List<SSTableName> getAllLocalTables(final SSTableName ssTableName) {
		final Collection<Integer> namprefixes = getAllRegionIds();
		
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
	protected List<SSTableName> convertRegionIdToTableNames(final SSTableName ssTableName,
			final Collection<Integer> regionIds) {
		
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
	public boolean addMapping(final int regionId, final BoundingBox boundingBox) {
				
		for(final RegionTablenameEntry regionTablenameEntry : regions) {
			// Mapping is known
			if(regionTablenameEntry.getRegionId() == regionId) {
				return false;
			}
		}
		
		regions.add(new RegionTablenameEntry(boundingBox, regionId));
		return true;
	}
	
	/**
	 * Remove a mapping
	 * @return
	 */
	public boolean removeMapping(final int regionId) {
		
		for (final Iterator<RegionTablenameEntry> iterator = regions.iterator(); iterator.hasNext(); ) {
			final RegionTablenameEntry regionTablenameEntry = (RegionTablenameEntry) iterator.next();
			
			if(regionTablenameEntry.getRegionId() == regionId) {
				regions.remove(regionTablenameEntry);
				return true;
			}
		}

		return false;
	}
	
	/**
	 * Remove all mappings
	 */
	public void clear() {
		regions.clear();
	}
	
}

class RegionTablenameEntry {
	protected final BoundingBox boundingBox;
	protected final int regionId;
	
	public RegionTablenameEntry(final BoundingBox boundingBox, final int regionId) {
		super();
		this.boundingBox = boundingBox;
		this.regionId = regionId;
	}

	public BoundingBox getBoundingBox() {
		return boundingBox;
	}

	public int getRegionId() {
		return regionId;
	}
	
	@Override
	public String toString() {
		return "RegionTablenameEntry [boundingBox=" + boundingBox + ", regionId=" + regionId + "]";
	}
}