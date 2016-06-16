package de.fernunihagen.dna.jkn.scalephant.distribution;

import java.util.HashMap;
import java.util.Map;

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
	public static DistributionRegion getGroupForGroupName(final String groupName) throws ZookeeperException {
		if(! groupGroupMap.containsKey(groupName)) {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
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
	public static DistributionRegion getGroupForTableName(final String tableName) throws ZookeeperException {
		if(! tableNameGroupMap.containsKey(tableName)) {
			final DistributionGroupName distributionGroupName = new DistributionGroupName(tableName);
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			final DistributionRegion distributionRegion = zookeeperClient.readDistributionGroup(distributionGroupName.getGroupname());
			tableNameGroupMap.put(tableName, distributionRegion);
		}
		
		return tableNameGroupMap.get(tableName);
	}
}
