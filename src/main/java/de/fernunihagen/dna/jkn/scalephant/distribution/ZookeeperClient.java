package de.fernunihagen.dna.jkn.scalephant.distribution;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.ScalephantService;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstanceManager;

public class ZookeeperClient implements ScalephantService, Watcher {
	
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
			registerScalephantInstance();
			readMembershipAndRegisterWatch();
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
	 * Register a watch on membership changes. A watch is a one-time operation, the watch
	 * is reregistered on each method call.
	 */
	protected void readMembershipAndRegisterWatch() {
		try {
			final List<String> instances = zookeeper.getChildren(getNodesPath(clustername), this);
			final DistributedInstanceManager distributedInstanceManager = DistributedInstanceManager.getInstance();
			
			final Set<DistributedInstance> instanceSet = new HashSet<DistributedInstance>();
			for(final String member : instances) {
				instanceSet.add(new DistributedInstance(member));
			}
			
			distributedInstanceManager.updateInstanceList(instanceSet);
			
		} catch (KeeperException | InterruptedException e) {
			logger.warn("Unable to read membership and create a watch", e);
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
			if(! wasEmpty) {
				sb.append(", ");
			}
			
			sb.append(zookeeperHost);
		}
	
		return sb.toString();
	}

	/**
	 * Zookeeper watched event
	 */
	@Override
	public void process(final WatchedEvent watchedEvent) {	
		
		logger.debug("Got zookeeper event: " + watchedEvent);
		
		// Ignore null parameter
		if(watchedEvent == null) {
			logger.warn("process called with an null argument");
			return;
		}
		
		// Ignore type=none event
		if(watchedEvent.getType() == EventType.None) {
			return;
		}
		
		// Process events
		if(watchedEvent.getPath() != null) {
			if(watchedEvent.getPath().equals(getNodesPath(clustername))) {
				readMembershipAndRegisterWatch();
			}
		} else {
			logger.warn("Got unknown zookeeper event: " + watchedEvent);
		}
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
			createDirectoryStructureIfNeeded();
			registerInstance();
		} catch (KeeperException | InterruptedException e) {
			logger.warn("Got exception while register to zookeeper", e);
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
		final String instanceZookeeperPath = getNodesPath(clustername) + "/" + ownInstanceName;
		logger.info("Register instance on: " + instanceZookeeperPath);
		zookeeper.create(instanceZookeeperPath, "".getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	/**
	 * Register the name of the cluster in the zookeeper directory
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	protected void createDirectoryStructureIfNeeded() throws KeeperException, InterruptedException {
		
		// /clusterpath
		final String clusterPath = getClusterPath(clustername);
		
		if(zookeeper.exists(clusterPath, this) == null) {
			logger.info(clusterPath + " not found, creating");
			zookeeper.create(clusterPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		// /clusterpath/nodes
		final String nodesPath = getNodesPath(clustername);
		if(zookeeper.exists(nodesPath, this) == null) {
			logger.info(nodesPath + " not found, creating");
			zookeeper.create(nodesPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
	
	/**
	 * Get the path of the zookeeper nodes
	 * @param clustername
	 * @return
	 */
	protected String getNodesPath(final String clustername) {
		return "/" + clustername + "/nodes";
	}

	@Override
	public String getServicename() {
		return "Zookeeper Client";
	}
	
}
