package de.fernunihagen.dna.jkn.scalephant.distribution;

import java.io.IOException;
import java.util.Collection;

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
	protected final Collection<String> zookeeperHosts;
	
	/**
	 * The name of the scalephant cluster
	 */
	protected final String clustername;
	
	/**
	 * The name of the local instance
	 */
	protected final String ownInstanceName;
	
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

	public ZookeeperClient(final Collection<String> zookeeperHosts, final String clustername, final String ownInstanceName) {
		super();
		this.zookeeperHosts = zookeeperHosts;
		this.clustername = clustername;
		this.ownInstanceName = ownInstanceName;
	}

	/**
	 * Connect to zookeeper
	 */
	@Override
	public void init() {
		if(zookeeperHosts == null || zookeeperHosts.isEmpty()) {
			logger.warn("No Zookeeper hosts are defined, not connecting to zookeeper");
			return;
		}
		
		try {
			zookeeper = new ZooKeeper(generateConnectString(), DEFAULT_TIMEOUT, this);
		} catch (IOException e) {
			logger.warn("Got exception while connecting to zookeeper", e);
		}
		
		try {
			registerInstance();
		} catch (InterruptedException | KeeperException e) {
			logger.warn("Unable to register the local instance in zookeeper");
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
		
		// No zookeeper hosts are defined
		if(zookeeperHosts == null) {
			logger.warn("No zookeeper hosts are defined");
			return "";
		}
		
		final StringBuilder sb = new StringBuilder();
		for(final String zookeeperHost : zookeeperHosts) {
			boolean wasEmpty = (sb.length() == 0);
			sb.append(zookeeperHost);
			if(wasEmpty) {
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
	protected boolean registerScalephantInstance() {
		if(zookeeper == null) {
			logger.warn("Register called but not connected to zookeeper");
			return false;
		}
		
		try {
			registerClusternameIfNeeded();
			registerInstance();
		} catch (KeeperException | InterruptedException e) {
			logger.warn("Got exception while reigster to zookeeper", e);
			return false;
		} 
		
		return true;
	}
	
	/**
	 * Register the scalephant instance
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	protected void registerInstance() throws KeeperException, InterruptedException {
		final String instanceZookeeperPath = getClusterPath(clustername) + "/" + ownInstanceName;
		logger.info("Register instance on: " + instanceZookeeperPath);
		zookeeper.create(instanceZookeeperPath, "".getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	/**
	 * Register the name of the cluster in the zookeeper directory
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	protected void registerClusternameIfNeeded() throws KeeperException, InterruptedException {
		
		final String clusterPath = getClusterPath(clustername);
		
		if(zookeeper.exists(clusterPath, this) == null) {
			logger.info(clusterPath + " not found, creating");
			zookeeper.create(clusterPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
