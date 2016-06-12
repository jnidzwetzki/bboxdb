package de.fernunihagen.dna.jkn.scalephant.distribution;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import de.fernunihagen.dna.jkn.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.jkn.scalephant.ScalephantConfigurationManager;

public class ZookeeperClientFactory {
	
	protected final static Map<ScalephantConfiguration, ZookeeperClient> instances;
	
	static {
		instances = new HashMap<ScalephantConfiguration, ZookeeperClient>();
	}
	
	/**
	 * Returns a new instance of the zookeeper client 
	 * @return
	 */
	public static ZookeeperClient getZookeeperClient() {
		final ScalephantConfiguration scalephantConfiguration = 
				ScalephantConfigurationManager.getConfiguration();
		
		return getZookeeperClient(scalephantConfiguration);
	}

	/**
	 * Returns a new instance of the zookeeper client (with a specific configuration)
	 * @param scalephantConfiguration
	 * @return
	 */
	public static ZookeeperClient getZookeeperClient(
			final ScalephantConfiguration scalephantConfiguration) {
		
		// Is an instance for the configuration known?
		if(instances.containsKey(scalephantConfiguration)) {
			return instances.get(scalephantConfiguration);
		}
		
		final Collection<String> zookeepernodes = scalephantConfiguration.getZookeepernodes();
		final String clustername = scalephantConfiguration.getClustername();
		final String localIp = scalephantConfiguration.getLocalip();
		final int localPort = scalephantConfiguration.getNetworkListenPort();
		
		final String instanceName = localIp + ":" + Integer.toString(localPort);
		
		final ZookeeperClient zookeeperClient = new ZookeeperClient(zookeepernodes, clustername);
		zookeeperClient.registerScalephantInstanceAfterConnect(instanceName);
		
		// Register instance
		instances.put(scalephantConfiguration, zookeeperClient);
		
		return zookeeperClient;
	}
}
