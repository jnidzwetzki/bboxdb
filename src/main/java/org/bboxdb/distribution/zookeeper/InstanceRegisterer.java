package org.bboxdb.distribution.zookeeper;

import java.util.function.Consumer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
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
		} catch (KeeperException e) {
			logger.error("Exception while registering instance", e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.error("Exception while registering instance", e);
		}
	}

	/**
	 * Update the instance data
	 * @param zookeeperClient
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	protected void updateStateData(final ZookeeperClient zookeeperClient) 
			throws KeeperException, InterruptedException {
		
		final ZooKeeper zookeeper = zookeeperClient.getZookeeper();
		
		final String statePath = zookeeperClient.getActiveInstancesPath() + "/" + instance.getStringValue();
		
		logger.info("Register instance on: {}", statePath);

		// Delete old state if exists (e.g. caused by a fast restart of the
		// service)
		if (zookeeper.exists(statePath, false) != null) {
			logger.debug("Old state path {} does exist, deleting", statePath);
			zookeeper.delete(statePath, -1);
		}

		// Register new state
		zookeeper.create(statePath, instance.getState().getZookeeperValue().getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	/**
	 * Update BBoxDB version
	 * @param zookeeperClient
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	protected void updateVersion(final ZookeeperClient zookeeperClient) 
			throws KeeperException, InterruptedException {
		
		final ZooKeeper zookeeper = zookeeperClient.getZookeeper();

		final String versionPath = zookeeperClient.getInstancesVersionPath() + "/" + instance.getStringValue();

		// Version
		if (zookeeper.exists(versionPath, false) != null) {
			zookeeper.setData(versionPath, Const.VERSION.getBytes(), -1);
		} else {
			zookeeper.create(versionPath, Const.VERSION.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}
	}
	
	/**
	 * Update the hardware info
	 * @param zookeeperClient
	 */
	protected void updateHardwareInfo(final ZookeeperClient zookeeperClient) {
		final ZooKeeper zookeeper = zookeeperClient.getZookeeper();

	}
}


