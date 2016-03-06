package de.fernunihagen.dna.jkn.scalephant.distribution;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;

public class ZookeeperClient implements Lifecycle, Watcher {
	
	/**
	 * The list of the zookeeper hosts
	 */
	protected final List<String> zookeeperHosts;
	
	/**
	 * The zookeeper client instance
	 */
	protected ZooKeeper zookeeper;
	
	/**
	 * The timeout for the zookeeper session
	 */
	protected final static int DEFAULT_TIMEOUT = 3000;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);


	public ZookeeperClient(final List<String> zookeeperHosts) {
		super();
		this.zookeeperHosts = zookeeperHosts;
	}

	/**
	 * Connect to zookeeper
	 */
	@Override
	public void init() {
		try {
			zookeeper = new ZooKeeper(generateConnectString(), DEFAULT_TIMEOUT, this);
		} catch (IOException e) {
			logger.warn("Got exception while connecting to zookeeper", e);
		}
	}

	/**
	 * Disconnect from zookeeper
	 */
	@Override
	public void shutdown() {
		if(zookeeper != null) {
			try {
				zookeeper.close();
			} catch (InterruptedException e) {
				logger.warn("Got exception while closing zookeeper connection", e);
			}
			zookeeper = null;
		}
	}
	
	/**
	 * Build a comma separated list of the zookeeper nodes
	 * @return
	 */
	protected String generateConnectString() {
		final StringBuilder sb = new StringBuilder();
		
		for(int i = 0; i < zookeeperHosts.size(); i++) {
			sb.append(zookeeperHosts.get(i));
			
			if(i != 0) {
				sb.append(", ");
			}
		}
		
		return sb.toString();
	}

	/**
	 * Zookeeper watched event
	 */
	@Override
	public void process(final WatchedEvent watchedEvent) {
	
	}
	
	/**
	 * Register this instance of the scalephant
	 * @param clustername
	 * @param ownInstanceName
	 */
	public boolean registerScalephantInstance(final String clustername, final String ownInstanceName) {
		if(zookeeper == null) {
			logger.warn("Register called but not connected to zookeeper");
			return false;
		}
		
		try {
			registerClusternameIfNeeded(clustername);
			registerInstance(clustername, ownInstanceName);
		} catch (KeeperException | InterruptedException e) {
			logger.warn("Got exception while reigster to zookeeper", e);
			return false;
		} 
		
		return true;
	}
	
	/**
	 * Register the scalephant instance
	 * @param clustername
	 * @param ownInstanceName
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	protected void registerInstance(final String clustername, final String ownInstanceName) throws KeeperException, InterruptedException {
		zookeeper.create(getClusterPath(clustername) + "/" + ownInstanceName, "".getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	/**
	 * Register the name of the cluster in the zookeeper directory
	 * @param clustername
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	protected void registerClusternameIfNeeded(final String clustername) throws KeeperException, InterruptedException {
		if(zookeeper.exists(getClusterPath(clustername), this) == null) {
			zookeeper.create(getClusterPath(clustername), "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}

	/**
	 * Get the path of the zookeeper clustername
	 * @param clustername
	 * @return
	 */
	protected String getClusterPath(final String clustername) {
		return "/" + clustername;
	}
	
}
