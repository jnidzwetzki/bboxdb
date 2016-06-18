package de.fernunihagen.dna.jkn.scalephant.distribution;

import java.util.HashMap;
import java.util.Map;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.SSTableName;

public class DistributionGroupCache {
	
	/**
	 * Mapping between the string group and the group object
	 */
	protected final static Map<String, DistributionRegion> groupGroupMap;
	
	/**
	 * Mapping between the table (as string) and the group object
	 */
	protected final static Map<String, DistributionRegion> tableNameGroupMap;

	static {
		groupGroupMap = new HashMap<String, DistributionRegion>();
		tableNameGroupMap = new HashMap<String, DistributionRegion>();
	}
	
	/**
	 * Get the distribution region for the given group name
	 * @param groupName
	 * @return
	 * @throws ZookeeperException 
	 */
	public static synchronized DistributionRegion getGroupForGroupName(final String groupName, final ZookeeperClient zookeeperClient) throws ZookeeperException {
		if(! groupGroupMap.containsKey(groupName)) {
			final DistributionRegion distributionRegion = zookeeperClient.readDistributionGroup(groupName);
			groupGroupMap.put(groupName, distributionRegion);
		}
		
		return groupGroupMap.get(groupName);
	}
	
	/**
	 * Get the distribution region for the given table name
	 * @param groupName
	 * @return
	 * @throws ZookeeperException 
	 */
	public static synchronized DistributionRegion getGroupForTableName(final String tableName, final ZookeeperClient zookeeperClient) throws ZookeeperException {
		if(! tableNameGroupMap.containsKey(tableName)) {
			final SSTableName ssTableName = new SSTableName(tableName);
			
			if(! ssTableName.isValid()) {
				throw new IllegalArgumentException("Invalid table name: " + tableName);
			}
			
			final DistributionRegion distributionRegion = zookeeperClient.readDistributionGroup(ssTableName.getDistributionGroup());
			tableNameGroupMap.put(tableName, distributionRegion);
		}
		
		return tableNameGroupMap.get(tableName);
	}
}
