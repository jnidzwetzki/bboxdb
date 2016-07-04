package de.fernunihagen.dna.jkn.scalephant.distribution.nameprefix;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.SSTableName;

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
	public Collection<SSTableName> getAllNameprefixesWithTable(final SSTableName ssTableName) {
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
	protected Collection<SSTableName> prefixNameprefixIntergerList(final SSTableName ssTableName,
			final Collection<Integer> namprefixes) {
		
		final List<SSTableName> result = new ArrayList<SSTableName>();
		
		for(final Integer nameprefix : namprefixes) {
			
			final SSTableName fullTableName = new SSTableName(
					ssTableName.getDimension(), 
					ssTableName.getDistributionGroup(), 
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
	public void addMapping(final int nameprefix, final BoundingBox boundingBox) {
		regions.add(new RegionTablenameEntry(boundingBox, nameprefix));
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