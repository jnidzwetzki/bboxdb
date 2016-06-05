package de.fernunihagen.dna.jkn.scalephant.distribution;

public class DistributionRegionFactory {

	/**
	 * The instance to the zookeeper client
	 */
	protected static ZookeeperClient zookeeperClient = null;
	
	/**
	 * The base level of a tree
	 */
	protected final static int BASE_LEVEL = 0;
	
	/**
	 * Factory method for a new root region
	 * @param name
	 * @return
	 */
	public static DistributionRegion createRootRegion(final String name) {
		final DistributionGroupName distributionGroupName = new DistributionGroupName(name);
		
		if(! distributionGroupName.isValid()) {
			throw new IllegalArgumentException("Invalid region name: " + name);
		}
		
		if(zookeeperClient != null) {
			return new DistributionRegionWithZookeeperIntegration(distributionGroupName, 
					BASE_LEVEL, new TotalLevel(), zookeeperClient);
		} else {
			return new DistributionRegion(distributionGroupName, BASE_LEVEL, new TotalLevel());

		}	
	}

	/**
	 * Get the zookeeper instance
	 * @return
	 */
	public static ZookeeperClient getZookeeperClient() {
		return zookeeperClient;
	}

	/**
	 * Set the zookeeper instance
	 * @param zookeeperClient
	 */
	public static void setZookeeperClient(final ZookeeperClient zookeeperClient) {
		DistributionRegionFactory.zookeeperClient = zookeeperClient;
	}	
}
