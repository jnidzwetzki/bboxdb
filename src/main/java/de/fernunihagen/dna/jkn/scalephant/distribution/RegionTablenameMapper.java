package de.fernunihagen.dna.jkn.scalephant.distribution;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;

public class RegionTablenameMapper {
	
	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient zookeeperClient;

	/**
	 * The mappings
	 */
	protected final List<RegionTablenameEntry> regions = new CopyOnWriteArrayList<RegionTablenameEntry>();
	
	
	public RegionTablenameMapper(final ZookeeperClient zookeeperClient) {
		super();
		this.zookeeperClient = zookeeperClient;
	}

	/**
	 * Search the table that covers the region
	 */
	public String getTablenameForRegion(final BoundingBox region) {

		for(final RegionTablenameEntry regionTablenameEntry : regions) {
			if(regionTablenameEntry.getBoundingBox().overlaps(region)) {
				return regionTablenameEntry.getTablename();
			}
		}
		
		return null;
	}
	
	/**
	 * Add a new mapping
	 * @param tablename
	 * @param boundingBox
	 */
	public void addMapping(final String tablename, final BoundingBox boundingBox) {
		regions.add(new RegionTablenameEntry(boundingBox, tablename));
	}
	
	/**
	 * Remove a mapping
	 * @return
	 */
	public boolean removeMapping(final String tablename) {
		for (final Iterator<RegionTablenameEntry> iterator = regions.iterator(); iterator.hasNext(); ) {
			final RegionTablenameEntry regionTablenameEntry = (RegionTablenameEntry) iterator.next();
			
			if(regionTablenameEntry.getTablename().equals(tablename)) {
				iterator.remove();
				return true;
			}
		}

		return false;
	}
	
	
}

class RegionTablenameEntry {
	protected BoundingBox boundingBox;
	protected String tablename;
	
	public RegionTablenameEntry(final BoundingBox boundingBox, final String tablename) {
		super();
		this.boundingBox = boundingBox;
		this.tablename = tablename;
	}

	public BoundingBox getBoundingBox() {
		return boundingBox;
	}

	public void setBoundingBox(final BoundingBox boundingBox) {
		this.boundingBox = boundingBox;
	}

	public String getTablename() {
		return tablename;
	}

	public void setTablename(final String tablename) {
		this.tablename = tablename;
	}

	@Override
	public String toString() {
		return "RegionTablenameEntry [boundingBox=" + boundingBox
				+ ", tablename=" + tablename + "]";
	}
	
}