package de.fernunihagen.dna.jkn.scalephant.distribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
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
	 * The zookeeper client instance
	 */
	protected ZooKeeper zookeeper;
	
	/**
	 * The name of the instance
	 */
	protected String instancename = null;
	
	/**
	 * The timeout for the zookeeper session
	 */
	protected final static int DEFAULT_TIMEOUT = 3000;
	
	/**
	 * The prefix for nodes in sequential queues
	 */
	protected final static String SEQUENCE_QUEUE_PREFIX = "id-";
	
	/**
	 * Name of the left tree node
	 */
	protected final static String NODE_LEFT = "left";
	
	/**
	 * Name of the right tree node
	 */
	protected final static String NODE_RIGHT = "right";
	
	/**
	 * Name of the split node
	 */
	protected final static String NAME_SPLIT = "split";
	
	/**
	 * Name of the name prefix node
	 */
	protected final static String NAME_NAMEPREFIX = "nameprefix";
	
	/**
	 * Name of the name prefix queue
	 */
	protected static final String NAME_PREFIXQUEUE = "nameprefixqueue";

	/**
	 * Name of the replication node
	 */
	protected final static String NAME_REPLICATION = "replication";
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);

	public ZookeeperClient(final Collection<String> zookeeperHosts, final String clustername) {
		super();
		this.zookeeperHosts = zookeeperHosts;
		this.clustername = clustername;
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
			createDirectoryStructureIfNeeded();
			
			if(instancename != null) {
				registerInstance();
			}
			
			readMembershipAndRegisterWatch();
		} catch (Exception e) {
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
		
			if(sb.length() != 0) {
				sb.append(",");
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
	public void registerScalephantInstanceAfterConnect(final String ownInstanceName) {
		this.instancename = ownInstanceName;
	}
	
	/**
	 * Register the scalephant instance
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	protected void registerInstance() throws ZookeeperException {
		final String instanceZookeeperPath = getNodesPath(clustername) + "/" + instancename;
		logger.info("Register instance on: " + instanceZookeeperPath);
		
		try {
			zookeeper.create(instanceZookeeperPath, 
					"".getBytes(), 
					ZooDefs.Ids.READ_ACL_UNSAFE, 
					CreateMode.EPHEMERAL);
		} catch (KeeperException | InterruptedException e) {
			throw new ZookeeperException(e);
		}
	}

	/**
	 * Register the name of the cluster in the zookeeper directory
	 * @throws ZookeeperException 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	protected void createDirectoryStructureIfNeeded() throws ZookeeperException {
		// /clusterpath/nodes - Node membership
		final String nodesPath = getNodesPath(clustername);
		createDirectoryStructureRecursive(nodesPath);
	}

	/**
	 * Get the path of the zookeeper clustername
	 * @param clustername
	 * @return
	 */
	protected String getClusterPath() {
		return "/" + clustername;
	}
	
	/**
	 * Get the path of the zookeeper nodes
	 * @param clustername
	 * @return
	 */
	protected String getNodesPath(final String clustername) {
		return getClusterPath() + "/nodes";
	}
	
	/**
	 * Get the path for the distribution group id queue
	 * @param distributionGroup
	 * @return
	 */
	protected String getDistributionGroupIdQueuePath(final String distributionGroup) {
		 return getDistributionGroupPath(distributionGroup) + "/" + NAME_PREFIXQUEUE;
	}
	
	/**
	 * Get the path for the distribution group
	 * @param distributionGroup
	 * @return
	 */
	protected String getDistributionGroupPath(final String distributionGroup) {
		return getClusterPath() + "/" + distributionGroup;
	}

	@Override
	public String getServicename() {
		return "Zookeeper Client";
	}
	
	/**
	 * Create the given directory structure recursive
	 * @param path
	 * @throws ZookeeperException 
	 */
	protected void createDirectoryStructureRecursive(final String path) throws ZookeeperException {
		
		try {
			// Does the full path already exists?
			if(zookeeper.exists(path, this) != null) {
				return;
			}
			
			// Otherwise check and create all sub paths
			final String[] allNodes = path.split("/");
			final StringBuilder sb = new StringBuilder();
			
			// Start by 1 to skip the initial / 
			for (int i = 1; i < allNodes.length; i++) {
				final String nextNode = allNodes[i];
				sb.append("/");
				sb.append(nextNode);
				
				final String partialPath = sb.toString();
							
				if(zookeeper.exists(partialPath, this) == null) {
					logger.info(partialPath + " not found, creating");
					
					zookeeper.create(partialPath, 
							"".getBytes(), 
							ZooDefs.Ids.OPEN_ACL_UNSAFE, 
							CreateMode.PERSISTENT);
				}
			}
		} catch (KeeperException | InterruptedException e) {
			throw new ZookeeperException(e);
		} 
	}
	
	/**
	 * Get the next table id for a given distribution group
	 * @return
	 * @throws ZookeeperException 
	 */
	public int getNextTableIdForDistributionGroup(final String distributionGroup) throws ZookeeperException {
		
		final String distributionGroupIdQueuePath = getDistributionGroupIdQueuePath(distributionGroup);
		
		createDirectoryStructureRecursive(distributionGroupIdQueuePath);
		
		try {	
			final String nodename = zookeeper.create(distributionGroupIdQueuePath + "/" + SEQUENCE_QUEUE_PREFIX, 
					"".getBytes(), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, 
					CreateMode.PERSISTENT_SEQUENTIAL);
			
			// Garbage collect the old child nodes
			doIdQueueGarbageCollection(distributionGroupIdQueuePath);
			
			// id-0000000063
			// Element 0: id-
			// Element 1: The number of the node
			final String[] splittedName = nodename.split(SEQUENCE_QUEUE_PREFIX);
			try {
				return Integer.parseInt(splittedName[1]);
			} catch(NumberFormatException e) {
				logger.warn("Unable to parse number: " + splittedName[1], e);
				throw new ZookeeperException(e);
			}
			
		} catch(InterruptedException | KeeperException e) {
			throw new ZookeeperException(e);
		}
	}

	/**
	 * Delete the old and unused child nodes for a given ID queue
	 * @param distributionGroupIdQueuePath
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	protected void doIdQueueGarbageCollection(final String distributionGroupIdQueuePath) throws KeeperException, InterruptedException {
		final Random random = new Random(System.currentTimeMillis());
		
		// 10 % garbage collection calls, 90% nop method calls
		if((random.nextInt() % 100) > 90) {
			
			logger.debug("Executing garbage collection on path: " + distributionGroupIdQueuePath);
			
			final List<String> childs = zookeeper.getChildren(distributionGroupIdQueuePath, false);
			for (Iterator<String> iterator = childs.iterator(); iterator.hasNext();) {
				final String nodeName = iterator.next();
				zookeeper.delete(distributionGroupIdQueuePath + "/" + nodeName, -1);
			}
		}
	}
	
	/**
	 * Read the structure of a distribution group
	 * @return
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	public DistributionRegion readDistributionGroup(final String distributionGroup) throws KeeperException, InterruptedException {
		final DistributionRegion root = DistributionRegion.createRootRegion(distributionGroup);
		final String path = getDistributionGroupPath(distributionGroup);
		
		readDistributionGroupRecursive(path, root);
		
		return root;
	}
	
	/**
	 * Read the distribution group in a recursive way
	 * @param path
	 * @param child
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	protected void readDistributionGroupRecursive(final String path, final DistributionRegion child) throws KeeperException, InterruptedException {
		
		final String splitPathName = path + "/" + NAME_SPLIT;
		
		if(zookeeper.exists(splitPathName, this) == null) {
			return;
		}
		
		final byte[] bytes = zookeeper.getData(splitPathName, false, null);
		final float splitFloat = Float.parseFloat(new String(bytes));
		
		child.setSplit(splitFloat);

		readDistributionGroupRecursive(path + "/" + NODE_LEFT, child.getLeftChild());
		readDistributionGroupRecursive(path + "/" + NODE_RIGHT, child.getRightChild());
	}
	
	/**
	 * Delete the node recursive
	 * @param path
	 * @throws KeeperException 
	 * @throws InterruptedException 
	 */
	protected void deleteNodesRecursive(final String path) throws InterruptedException, KeeperException {
		
		final List<String> childs = zookeeper.getChildren(path, false);
		
		for(final String child: childs) {
			deleteNodesRecursive(path + "/"+ child);
		}
		
		zookeeper.delete(path, -1);
	}
	
	/**
	 * Create a new distribution group
	 * @param distributionGroup
	 * @param replicationFactor
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * @throws ZookeeperException 
	 */
	public void createDistributionGroup(final String distributionGroup, final short replicationFactor) throws KeeperException, InterruptedException, ZookeeperException {
		final String path = getDistributionGroupPath(distributionGroup);
		
		zookeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		final int nameprefix = getNextTableIdForDistributionGroup(distributionGroup);
		
		zookeeper.create(path + "/" + NAME_NAMEPREFIX, Integer.toString(nameprefix).getBytes(), 
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		zookeeper.create(path + "/" + NAME_REPLICATION, Short.toString(replicationFactor).getBytes(), 
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);	
	}
	
	/**
	 * Delete an existing distribution group
	 * @param distributionGroup
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void deleteDistributionGroup(final String distributionGroup) throws InterruptedException, KeeperException {
		final String path = getDistributionGroupPath(distributionGroup);
		deleteNodesRecursive(path);
	}
	
	/**
	 * List all existing distribution groups
	 * @return
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	public List<DistributionGroupName> getDistributionGroups() throws KeeperException, InterruptedException {
		final List<DistributionGroupName> groups = new ArrayList<DistributionGroupName>();
		
		final String clusterPath = getClusterPath();
		final List<String> nodes = zookeeper.getChildren(clusterPath, false);
		
		for(final String node : nodes) {
			final DistributionGroupName groupName = new DistributionGroupName(node);
			if(groupName.isValid()) {
				groups.add(groupName);
			} else {
				logger.warn("Got invalid distribution group name from zookeeper: " + groupName);
			}
		}
		
		return groups;
	}
	
	/**
	 * Get the replication factor for a distribution group
	 * @param distributionGroup
	 * @return
	 * @throws ZookeeperException
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public short getReplicationFactorForDistributionGroup(final String distributionGroup) throws ZookeeperException, KeeperException, InterruptedException {
		
		final String path = getDistributionGroupPath(distributionGroup);
		final String fullPath = path + "/" + NAME_REPLICATION;
		final byte[] data = zookeeper.getData(fullPath, false, null);
		
		try {
			return Short.parseShort(new String(data));
		} catch (NumberFormatException e) {
			throw new ZookeeperException("Unable to parse replication factor: " + data + " for " + fullPath);
		}
	}

}
