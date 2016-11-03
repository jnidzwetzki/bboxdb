package de.fernunihagen.dna.scalephant.distribution;

import java.util.HashMap;
import java.util.Map;

import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperException;
import de.fernunihagen.dna.scalephant.network.client.ScalephantException;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;

public class DistributionGroupCache {
	
	/**
	 * Mapping between the string group and the group object
	 */
	protected final static Map<String, DistributionRegion> groupGroupMap;

	static {
		groupGroupMap = new HashMap<String, DistributionRegion>();
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
	 * @throws ScalephantException 
	 */
	public static synchronized DistributionRegion getGroupForTableName(final String tableName, final ZookeeperClient zookeeperClient) throws ZookeeperException, ScalephantException {
		final SSTableName ssTableName = new SSTableName(tableName);
		
		if(! ssTableName.isValid()) {
			throw new ScalephantException("Invalid tablename: " + tableName);
		}
		
		return getGroupForGroupName(ssTableName.getDistributionGroup(), zookeeperClient);
	}
}
