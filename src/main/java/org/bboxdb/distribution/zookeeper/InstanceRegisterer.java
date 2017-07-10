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
	protected final DistributedInstance instancename;

	public InstanceRegisterer() {
		instancename = ZookeeperClientFactory.getLocalInstanceName();
	}

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);
	
	@Override
	public void accept(final ZookeeperClient zookeeperClient) {
		if (instancename == null) {
			return;
		}

		final ZooKeeper zookeeper = zookeeperClient.getZookeeper();
		
		final String statePath = zookeeperClient.getActiveInstancesPath() + "/" + instancename.getStringValue();
		final String versionPath = zookeeperClient.getInstancesVersionPath() + "/" + instancename.getStringValue();
		
		logger.info("Register instance on: {}", statePath);

		try {
			// Version
			if (zookeeper.exists(versionPath, false) != null) {
				zookeeper.setData(versionPath, Const.VERSION.getBytes(), -1);
			} else {
				zookeeper.create(versionPath, Const.VERSION.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			}

			// Delete old state if exists (e.g. caused by a fast restart of the
			// service)
			if (zookeeper.exists(statePath, false) != null) {
				logger.debug("Old state path {} does exist, deleting", statePath);
				zookeeper.delete(statePath, -1);
			}

			// Register new state
			zookeeper.create(statePath, instancename.getState().getZookeeperValue().getBytes(),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (KeeperException e) {
			logger.error("Exception while registering instance", e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.error("Exception while registering instance", e);
		}

	}
}


