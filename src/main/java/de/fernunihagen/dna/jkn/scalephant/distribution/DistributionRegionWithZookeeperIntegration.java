package de.fernunihagen.dna.jkn.scalephant.distribution;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class DistributionRegionWithZookeeperIntegration extends DistributionRegion implements Watcher {

	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient zookeeperClient;
	
	public DistributionRegionWithZookeeperIntegration(final DistributionGroupName name, final int level, final TotalLevel totalLevel, final ZookeeperClient zookeeperClient) {
		super(name, level, totalLevel);
		this.zookeeperClient = zookeeperClient;
	}

	/**
	 * Process structure updates (e.g. changes in the distribution group)
	 */
	@Override
	public void process(final WatchedEvent event) {
		
	}
	
	/**
	 * Create a new instance of this type
	 */
	@Override
	protected DistributionRegion createNewInstance() {
		return new DistributionRegionWithZookeeperIntegration(distributionGroupName, level, totalLevel, zookeeperClient);
	}
}
