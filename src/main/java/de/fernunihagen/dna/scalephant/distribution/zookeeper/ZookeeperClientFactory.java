package de.fernunihagen.dna.scalephant.distribution.zookeeper;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import de.fernunihagen.dna.scalephant.Const;
import de.fernunihagen.dna.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;

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

		final ZookeeperClient zookeeperClient = new ZookeeperClient(zookeepernodes, clustername);
		final DistributedInstance instance = getLocalInstanceName(scalephantConfiguration);
		zookeeperClient.registerScalephantInstanceAfterConnect(instance);
		
		// Register instance
		instances.put(scalephantConfiguration, zookeeperClient);
		
		return zookeeperClient;
	}

	/**
	 * Get the name of the local instance
	 * @param scalephantConfiguration
	 * @return
	 */
	public static DistributedInstance getLocalInstanceName(final ScalephantConfiguration scalephantConfiguration) {
		final String localIp = scalephantConfiguration.getLocalip();
		final int localPort = scalephantConfiguration.getNetworkListenPort();
		final DistributedInstance instance = new DistributedInstance(localIp, localPort, Const.VERSION);
		return instance;
	}

	/**
	 * Get a zookeeoer instance and init if needed
	 * @return
	 */
	public static ZookeeperClient getZookeeperClientAndInit() {
		final ZookeeperClient zookeeperClient = getZookeeperClient();
		
		if(! zookeeperClient.isConnected()) {
			zookeeperClient.init();
		}
		
		return zookeeperClient;
	}
}
