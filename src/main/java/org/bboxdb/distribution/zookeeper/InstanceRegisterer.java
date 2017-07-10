package org.bboxdb.distribution.zookeeper;

import java.util.function.Consumer;

import org.apache.zookeeper.ZooKeeper;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.misc.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstanceRegisterer implements Consumer<ZookeeperClient> {
	
	/**
	 * The name of the instance
	 */
	protected final DistributedInstance instance;

	public InstanceRegisterer() {
		instance = ZookeeperClientFactory.getLocalInstanceName();
	}
	
	public InstanceRegisterer(final DistributedInstance instance) {
		this.instance = instance;
	}
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);
	
	@Override
	public void accept(final ZookeeperClient zookeeperClient) {
		
		if (instance == null) {
			logger.error("Unable to determine local instance name");
			return;
		}

		try {
			updateStateData(zookeeperClient);
			updateVersion(zookeeperClient);
			updateHardwareInfo(zookeeperClient);
		} catch (ZookeeperException e) {
			logger.error("Exception while registering instance", e);
		}
	}

	/**
	 * Update the instance data
	 * @param zookeeperClient
	 * @throws ZookeeperException 
	 */
	protected void updateStateData(final ZookeeperClient zookeeperClient) throws ZookeeperException {
		
		final String statePath = zookeeperClient.getActiveInstancesPath() + "/" + instance.getStringValue();
		
		logger.info("Register instance on: {}", statePath);
		zookeeperClient.replaceEphemeralNode(statePath, instance.getState().getZookeeperValue().getBytes());
	}

	/**
	 * Update BBoxDB version
	 * @param zookeeperClient
	 * @throws ZookeeperException 
	 */
	protected void updateVersion(final ZookeeperClient zookeeperClient) throws ZookeeperException {
		final String versionPath = zookeeperClient.getInstancesVersionPath() + "/" + instance.getStringValue();
		zookeeperClient.replacePersistentNode(versionPath, Const.VERSION.getBytes());
	}
	
	/**
	 * Update the hardware info
	 * @param zookeeperClient
	 */
	protected void updateHardwareInfo(final ZookeeperClient zookeeperClient) {
		final ZooKeeper zookeeper = zookeeperClient.getZookeeper();

	}
}


