package de.fernunihagen.dna.jkn.scalephant.distribution.nameprefix;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;

public class NameprefixMapper {

	/**
	 * The mappings
	 */
	protected final List<RegionTablenameEntry> regions = new CopyOnWriteArrayList<RegionTablenameEntry>();
	
	
	public NameprefixMapper() {
		super();
	}

	/**
	 * Search the nameprefixes that are overlapped by the bounding box
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