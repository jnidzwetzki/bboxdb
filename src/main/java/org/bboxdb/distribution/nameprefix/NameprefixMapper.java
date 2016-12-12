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
package org.bboxdb.distribution.nameprefix;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NameprefixMapper {

	/**
	 * The mappings
	 */
	protected final List<RegionTablenameEntry> regions = new CopyOnWriteArrayList<RegionTablenameEntry>();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(NameprefixMapper.class);
	
	
	public NameprefixMapper() {
		super();
	}

	/**
	 * Search the name prefixes that are overlapped by the bounding box
	 */
	public Collection<Integer> getNameprefixesForRegion(final BoundingBox region) {

		final List<Integer> result = new ArrayList<Integer>();
		
		for(final RegionTablenameEntry regionTablenameEntry : regions) {
			if(regionTablenameEntry.getBoundingBox().overlaps(region)) {
				result.add(regionTablenameEntry.getNameprefix());
			}
		}
		
		return result;
	}
	
	/**
	 * Get all name prefixes
	 * @return
	 */
	public Collection<Integer> getAllNamePrefixes() {
		final List<Integer> result = new ArrayList<Integer>(regions.size());
		
		for(final RegionTablenameEntry regionTablenameEntry : regions) {
			result.add(regionTablenameEntry.getNameprefix());
		}
		
		return result;
	}
	
	/**
	 * Get the SSTables that are responsible for a given bounding box
	 * @param region
	 * @param ssTableName
	 * @return
	 */
	public Collection<SSTableName> getNameprefixesForRegionWithTable(final BoundingBox region, final SSTableName ssTableName) {
		final Collection<Integer> namprefixes = getNameprefixesForRegion(region);
		
		if(namprefixes.isEmpty() && logger.isWarnEnabled()) {
			logger.warn("Got an empty result list by query region: " + region);
		}
		
		return prefixNameprefixIntergerList(ssTableName, namprefixes);
	}
	
	
	/**
	 * Get all SSTables that are stored local
	 * @param ssTableName
	 * @return
	 */
	public List<SSTableName> getAllNameprefixesWithTable(final SSTableName ssTableName) {
		final Collection<Integer> namprefixes = getAllNamePrefixes();
		
		if(namprefixes.isEmpty() && logger.isWarnEnabled()) {
			logger.warn("Got an empty result list by query all regions");
		}
		
		return prefixNameprefixIntergerList(ssTableName, namprefixes);		
	}

	/**
	 * Prefix all entries of the given list with the name of the sstable
	 * @param ssTableName
	 * @param namprefixes
	 * @return
	 */
	protected List<SSTableName> prefixNameprefixIntergerList(final SSTableName ssTableName,
			final Collection<Integer> namprefixes) {
		
		final List<SSTableName> result = new ArrayList<SSTableName>();
		
		for(final Integer nameprefix : namprefixes) {
			
			final SSTableName fullTableName = new SSTableName(
					ssTableName.getDimension(), 
					ssTableName.getGroup(), 
					ssTableName.getTablename(), 
					nameprefix);
			
			result.add(fullTableName);
		}
		
		return result;
	}
	
	/**
	 * Add a new mapping
	 * @param tablename
	 * @param boundingBox
	 */
	public boolean addMapping(final int nameprefix, final BoundingBox boundingBox) {
				
		for(final RegionTablenameEntry regionTablenameEntry : regions) {
			// Mapping is known
			if(regionTablenameEntry.getNameprefix() == nameprefix) {
				return false;
			}
		}
		
		regions.add(new RegionTablenameEntry(boundingBox, nameprefix));
		return true;
	}
	
	/**
	 * Remove a mapping
	 * @return
	 */
	public boolean removeMapping(final int nameprefix) {
		for (final Iterator<RegionTablenameEntry> iterator = regions.iterator(); iterator.hasNext(); ) {
			final RegionTablenameEntry regionTablenameEntry = (RegionTablenameEntry) iterator.next();
			
			if(regionTablenameEntry.getNameprefix() == nameprefix) {
				iterator.remove();
				return true;
			}
		}

		return false;
	}
	
}

class RegionTablenameEntry {
	protected final BoundingBox boundingBox;
	protected final int nameprefix;
	
	public RegionTablenameEntry(final BoundingBox boundingBox, final int nameprefix) {
		super();
		this.boundingBox = boundingBox;
		this.nameprefix = nameprefix;
	}

	public BoundingBox getBoundingBox() {
		return boundingBox;
	}

	public int getNameprefix() {
		return nameprefix;
	}
	
	@Override
	public String toString() {
		return "RegionTablenameEntry [boundingBox=" + boundingBox + ", nameprefix=" + nameprefix + "]";
	}
}