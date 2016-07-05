package de.fernunihagen.dna.jkn.scalephant.distribution;

import java.util.ArrayList;
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
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Const;
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
	protected DistributedInstance instancename = null;
	
	/**
	 * Is the membership observer active?
	 */
	protected volatile boolean membershipObserver = false;
	
	/**
	 * The timeout for the zookeeper session
	 */
	protected final static int DEFAULT_TIMEOUT = 3000;
	

	
	public class NodeNames {
		/**
		 * The prefix for nodes in sequential queues
		 */
		public final static String SEQUENCE_QUEUE_PREFIX = "id-";
		
		/**
		 * Name of the left tree node
		 */
		public final static String NAME_LEFT = "left";
		
		/**
		 * Name of the right tree node
		 */
		public final static String NAME_RIGHT = "right";
		
		/**
		 * Name of the split node
		 */
		public final static String NAME_SPLIT = "split";
		
		/**
		 * Name of the name prefix node
		 */
		public final static String NAME_NAMEPREFIX = "nameprefix";
		
		/**
		 * Name of the name prefix queue
		 */
		public static final String NAME_PREFIXQUEUE = "nameprefixqueue";
	
		/**
		 * Name of the replication node
		 */
		public final static String NAME_REPLICATION = "replication";
		
		/**
		 * Name of the systems node
		 */
		public final static String NAME_SYSTEMS = "systems";
		
		/**
		 * Name of the version node
		 */
		public final static String NAME_VERSION = "version";
		
		/**
		 * Name of the state node
		 */
		public final static String NAME_STATE = "state";
	}
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);

	public ZookeeperClient(final Collection<String> zookeeperHosts, final String clustername) {
		super();
		this.zookeeperHosts = zookeeperHosts;
		this.clustername = clustername;
		
		// Set the zookeeper instance for self updating data structures
		DistributionRegionFactory.setZookeeperClient(this);
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
			
			registerInstanceIfNameWasSet();
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
				logger.info("Disconnecting from zookeeper");
				zookeeper.close();
			} catch (InterruptedException e) {
				logger.warn("Got exception while closing zookeeper connection", e);
			}
			zookeeper = null;
		}
	}
	
	/**
	 * Start the membership observer
	 */
	public void startMembershipObserver() {
		membershipObserver = true;
		readMembershipAndRegisterWatch();
	}
	
	public void stopMembershipObserver() {
		membershipObserver = false;
	}
	
	/**
	 * Register a watch on membership changes. A watch is a one-time operation, the watch
	 * is reregistered on each method call.
	 */
	protected void readMembershipAndRegisterWatch() {
		
		if(!membershipObserver) {
			logger.info("Ignore membership event, because observer is not active");
			return;
		}
		
		try {
			final String nodesPath = getNodesPath(clustername);
			final List<String> instances = zookeeper.getChildren(nodesPath, this);
			final DistributedInstanceManager distributedInstanceManager = DistributedInstanceManager.getInstance();
			
			final Set<DistributedInstance> instanceSet = new HashSet<DistributedInstance>();
			for(final String member : instances) {
				
				final String versionPath = nodesPath + "/" + member;
				String versionName = DistributedInstance.UNKOWN_VERSION; 
				
				try {
					versionName = readPathAndReturnString(versionPath, true);
				} catch (ZookeeperException e) {
					logger.info("Unable to read version for: " + versionPath);
				}
				
				final DistributedInstance theInstance = new DistributedInstance(member, versionName); 
				instanceSet.add(theInstance);
			}
			
			distributedInstanceManager.updateInstanceList(instanceSet);
			
		} catch (KeeperException | InterruptedException e) {
			logger.warn("Unable to read membership and create a watch", e);
		}
	}
	
	
	/**
	 * Get the children and register a watch
	 * @param path
	 * @param watcher
	 * @return
	 * @throws ZookeeperException 
	 */
	public List<String> getChildren(final String path, final Watcher watcher) throws ZookeeperException {
		try {
			
			if(zookeeper.exists(path, false) == null) {
				return null;
			}
			
			return zookeeper.getChildren(path, watcher);
		} catch (KeeperException | InterruptedException e) {
			throw new ZookeeperException(e);
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
	 * @param localIp
	 * @param localPort
	 */
	public void registerScalephantInstanceAfterConnect(final String localIp, final Integer localPort) {
		this.instancename = new DistributedInstance(localIp, localPort, Const.VERSION);
	}

	/**
	 * Register the scalephant instance
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	protected void registerInstanceIfNameWasSet() throws ZookeeperException {
		
		if(instancename == null) {
			return;
		}

		final String instanceZookeeperPath = getNodesPath(clustername) + "/" + instancename.getStringValue();
		
		logger.info("Register instance on: " + instanceZookeeperPath);
		
		try {
			zookeeper.create(instanceZookeeperPath, Const.VERSION.getBytes(), 
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
		 return getDistributionGroupPath(distributionGroup) + "/" + NodeNames.NAME_PREFIXQUEUE;
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
			if(zookeeper.exists(path, false) != null) {
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
							
				if(zookeeper.exists(partialPath, false) == null) {
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
			final String nodename = createPersistentSequencialNode(distributionGroupIdQueuePath + "/" 
					+ NodeNames.SEQUENCE_QUEUE_PREFIX, "".getBytes());
			
			// Delete the created node
			logger.debug("Got new table id; deleting node: " + nodename);
			zookeeper.delete(nodename, -1);
			
			// id-0000000063
			// Element 0: id-
			// Element 1: The number of the node
			final String[] splittedName = nodename.split(NodeNames.SEQUENCE_QUEUE_PREFIX);
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
	 * Read the structure of a distribution group
	 * @return
	 * @throws ZookeeperException 
	 */
	public DistributionRegion readDistributionGroup(final String distributionGroup) throws ZookeeperException {
		try {
			final String path = getDistributionGroupPath(distributionGroup);

			if(zookeeper.exists(path, false) == null) {
				throw new ZookeeperException("Unable to read: " + distributionGroup + " path " + path + " does not exist");
			}
			
			final DistributionRegion root = DistributionRegionFactory.createRootRegion(distributionGroup);

			readDistributionGroupRecursive(path, root);
			
			return root;
		} catch (KeeperException | InterruptedException e) {
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * Read the distribution group in a recursive way
	 * @param path
	 * @param child
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	protected void readDistributionGroupRecursive(final String path, final DistributionRegion child) throws ZookeeperException {
			
			final int namePrefix = getNamePrefixForPath(path);
			child.setNameprefix(namePrefix);

			child.setSystems(getSystemsForDistributionRegion(child));
			child.setState(getStateForDistributionRegion(path));

			// If the node is not split, stop recursion
			if(! isGroupSplitted(path)) {
				return;
			}
			
			final float splitFloat = getSplitPositionForPath(path);
			child.setSplit(splitFloat, false);
			
			readDistributionGroupRecursive(path + "/" + NodeNames.NAME_LEFT, child.getLeftChild());
			readDistributionGroupRecursive(path + "/" + NodeNames.NAME_RIGHT, child.getRightChild());
	}

	/**
	 * Get the name prefix for a given path
	 * @param path
	 * @return
	 * @throws ZookeeperException 
	 */
	protected int getNamePrefixForPath(final String path) throws ZookeeperException {
		
		final String namePrefixPath = path + "/" + NodeNames.NAME_NAMEPREFIX;
		String namePrefix = null;
		
		try {
			namePrefix = readPathAndReturnString(namePrefixPath, false);
			return Integer.parseInt(namePrefix);
		} catch (NumberFormatException e) {
			throw new ZookeeperException("Unable to parse name prefix '" + namePrefix + "' for " + namePrefixPath);
		}		
	}
	
	/**
	 * Get the state for a given path
	 * @throws ZookeeperException 
	 */
	public String getStateForDistributionRegion(final String path) throws ZookeeperException {
		final String statePath = path + "/" + NodeNames.NAME_STATE;
		return readPathAndReturnString(statePath, false);
	}
	
	/**
	 * Get the state for a given path
	 * @return 
	 * @throws ZookeeperException 
	 */
	public String getStateForDistributionRegion(final DistributionRegion region) throws ZookeeperException  {
		final String path = getZookeeperPathForDistributionRegion(region);
		return getStateForDistributionRegion(path);
	}
	
	/**
	 * Set the state for a given path
	 * @param path
	 * @param state
	 * @throws ZookeeperException 
	 */
	public void setStateForDistributionGroup(final String path, final String state) throws ZookeeperException  {
		final String statePath = path + "/" + NodeNames.NAME_STATE;
		try {
			zookeeper.setData(statePath, state.getBytes(), -1);
		} catch (KeeperException | InterruptedException e) {
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * Set the state for a given distribution region
	 * @param region
	 * @param state
	 * @throws ZookeeperException
	 */
	public void setStateForDistributionGroup(final DistributionRegion region, final String state) throws ZookeeperException  {
		final String path = getZookeeperPathForDistributionRegion(region);
		setStateForDistributionGroup(path, state);
	}

	
	/**
	 * Test weather the group path is split or not
	 * @param path
	 * @return
	 * @throws ZookeeperException 
	 */
	protected boolean isGroupSplitted(final String path) throws ZookeeperException {
		try {
			final String splitPathName = path + "/" + NodeNames.NAME_SPLIT;
			if(zookeeper.exists(splitPathName, false) == null) {
				return false;
			} else {
				return true;
			}
		} catch (KeeperException | InterruptedException e) {
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * Get the split position for a given path
	 * @param path
	 * @return
	 * @throws ZookeeperException
	 */
	protected float getSplitPositionForPath(final String path) throws ZookeeperException  {
		
		final String splitPathName = path + "/" + NodeNames.NAME_SPLIT;
		String splitString = null;
		
		try {			
			splitString = readPathAndReturnString(splitPathName, false);
			return Float.parseFloat(splitString);
		} catch (NumberFormatException e) {
			throw new ZookeeperException("Unable to parse split pos '" + splitString + "' for " + splitPathName);
		}		
	}

	/**
	 * Read the given path and returns a string result
	 * @param pathName
	 * @return
	 * @throws ZookeeperException
	 */
	protected String readPathAndReturnString(final String pathName, final boolean retry) throws ZookeeperException {
		try {
			if(zookeeper.exists(pathName, false) == null) {
				if(retry != true) {
					throw new ZookeeperException("The path does not exist: " + pathName);
				} else {
					Thread.sleep(500);
					if(zookeeper.exists(pathName, false) == null) {
						throw new ZookeeperException("The path does not exist: " + pathName);
					}
				}
			}
		
			final byte[] bytes = zookeeper.getData(pathName, false, null);
			return new String(bytes);
		} catch(InterruptedException | KeeperException e) {
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * Delete the node recursive
	 * @param path
	 * @throws ZookeeperException 
	 */
	protected void deleteNodesRecursive(final String path) throws ZookeeperException {
		try {
			
			final List<String> childs = zookeeper.getChildren(path, false);
			
			for(final String child: childs) {
				deleteNodesRecursive(path + "/"+ child);
			}
			
			zookeeper.delete(path, -1);
			
		} catch (InterruptedException e) {
			throw new ZookeeperException(e);
		} catch (KeeperException e) {
			if(e.code() == KeeperException.Code.NONODE) {
				// We try to delete concurrently deleted
				// nodes. So we can ignore the exception.
			} else {
				throw new ZookeeperException(e);
			}
		}
	}
	
	/**
	 * Create a new distribution group
	 * @param distributionGroup
	 * @param replicationFactor
	 * @throws ZookeeperException 
	 */
	public void createDistributionGroup(final String distributionGroup, final short replicationFactor) throws ZookeeperException {
		try {
			final String path = getDistributionGroupPath(distributionGroup);
			
			zookeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			
			final int nameprefix = getNextTableIdForDistributionGroup(distributionGroup);
						
			zookeeper.create(path + "/" + NodeNames.NAME_NAMEPREFIX, Integer.toString(nameprefix).getBytes(), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			
			zookeeper.create(path + "/" + NodeNames.NAME_REPLICATION, Short.toString(replicationFactor).getBytes(), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			
			zookeeper.create(path + "/" + NodeNames.NAME_SYSTEMS, "".getBytes(), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

			zookeeper.create(path + "/" + NodeNames.NAME_VERSION, Long.toString(System.currentTimeMillis()).getBytes(), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			
			zookeeper.create(path + "/" + NodeNames.NAME_STATE, DistributionRegion.STATE_CREATED.getBytes(), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			
		} catch (KeeperException | InterruptedException e) {
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * Delete an existing distribution group
	 * @param distributionGroup
	 * @throws ZookeeperException 
	 */
	public void deleteDistributionGroup(final String distributionGroup) throws ZookeeperException {
		
		// Does the path not exist? We are done!
		if(! isDistributionGroupRegistered(distributionGroup)) {
			return;
		}
		
		final String path = getDistributionGroupPath(distributionGroup);			
		deleteNodesRecursive(path);
	}
	
	/**
	 * Does the distribution group exists?
	 * @param distributionGroup
	 * @return 
	 * @throws ZookeeperException
	 */
	public boolean isDistributionGroupRegistered(final String distributionGroup) throws ZookeeperException {
		final String path = getDistributionGroupPath(distributionGroup);

		try {
			// Does the path not exist? We are done!
			if(zookeeper.exists(path, false) == null) {
				return false;
			}
			
			return true;
		} catch (KeeperException | InterruptedException e) {
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * List all existing distribution groups
	 * @return
	 * @throws ZookeeperException 
	 */
	public List<DistributionGroupName> getDistributionGroups() throws ZookeeperException {
		try {
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
		} catch (KeeperException | InterruptedException e) {
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * Get the replication factor for a distribution group
	 * @param distributionGroup
	 * @return
	 * @throws ZookeeperException
	 */
	public short getReplicationFactorForDistributionGroup(final String distributionGroup) throws ZookeeperException {
		
		try {
			final String path = getDistributionGroupPath(distributionGroup);
			final String fullPath = path + "/" + NodeNames.NAME_REPLICATION;
			final byte[] data = zookeeper.getData(fullPath, false, null);
			
			try {
				return Short.parseShort(new String(data));
			} catch (NumberFormatException e) {
				throw new ZookeeperException("Unable to parse replication factor: " + data + " for " + fullPath);
			}
		} catch (KeeperException | InterruptedException e) {
			throw new ZookeeperException(e);
		} 
	}
	
	/**
	 * Get the version number of the distribution group
	 * @param distributionGroup
	 * @return
	 * @throws ZookeeperException
	 */
	public String getVersionForDistributionGroup(final String distributionGroup) throws ZookeeperException {
		final String path = getDistributionGroupPath(distributionGroup);
		final String fullPath = path + "/" + NodeNames.NAME_VERSION;
		return readPathAndReturnString(fullPath, false);	 
	}

	/**
	 * Create a new persistent node
	 * @param path
	 * @param bytes
	 * @return 
	 * @throws ZookeeperException 
	 */
	public String createPersistentNode(final String path, final byte[] bytes) throws ZookeeperException {
		try {
			return zookeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException | InterruptedException e) {
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * Create a new persistent sequential node
	 * @param path
	 * @param bytes
	 * @return 
	 * @throws ZookeeperException 
	 */
	public String createPersistentSequencialNode(final String path, final byte[] bytes) throws ZookeeperException {
		try {
			return zookeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		} catch (KeeperException | InterruptedException e) {
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * Get the zookeeper path for a distribution region
	 * @param distributionRegion
	 * @return
	 */
	protected String getZookeeperPathForDistributionRegion(final DistributionRegion distributionRegion) {
		final String name = distributionRegion.getName();
		final StringBuilder sb = new StringBuilder();
		
		DistributionRegion tmpRegion = distributionRegion;
		while(tmpRegion.getParent() != null) {
			if(tmpRegion.isLeftChild()) {
				sb.insert(0, "/" + NodeNames.NAME_LEFT);
			} else {
				sb.insert(0, "/" + NodeNames.NAME_RIGHT);
			}
			
			tmpRegion = tmpRegion.getParent();
		}
		
		sb.insert(0, getDistributionGroupPath(name));
		return sb.toString();
	}
	
	/**
	 * Get the systems for the distribution region
	 * @param region
	 * @return
	 * @throws ZookeeperException 
	 */
	public Collection<DistributedInstance> getSystemsForDistributionRegion(final DistributionRegion region) throws ZookeeperException {
		try {
			final Set<DistributedInstance> result = new HashSet<DistributedInstance>();
			final String path = getZookeeperPathForDistributionRegion(region) + "/" + NodeNames.NAME_SYSTEMS;
			
			// Does the requested node exists?
			if(zookeeper.exists(path, false) == null) {
				return null;
			}
			
			final List<String> childs = zookeeper.getChildren(path, false);
			
			for(final String childName : childs) {
				final byte[] data = zookeeper.getData(path + "/" + childName, false, null);
				final String systemName = new String(data);
				result.add(new DistributedInstance(systemName));
			}
			
			return result;
		} catch (KeeperException | InterruptedException e) {
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * Add a system to a distribution region
	 * @param region
	 * @param system
	 * @throws ZookeeperException 
	 */
	public void addSystemToDistributionRegion(final DistributionRegion region, final DistributedInstance system) throws ZookeeperException {
		
		if(system == null) {
			throw new IllegalArgumentException("Unable to add system with value null");
		}
		
		try {
			final String path = getZookeeperPathForDistributionRegion(region) + "/" + NodeNames.NAME_SYSTEMS + "/" + NodeNames.SEQUENCE_QUEUE_PREFIX;
			logger.debug("Register system under systems node: " + path);
			createPersistentSequencialNode(path, system.getStringValue().getBytes());
		} catch (ZookeeperException e) {
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * Delete a system to a distribution region
	 * @param region
	 * @param system
	 * @return 
	 * @throws ZookeeperException 
	 */
	public boolean deleteSystemFromDistributionRegion(final DistributionRegion region, final DistributedInstance system) throws ZookeeperException {
		
		if(system == null) {
			throw new IllegalArgumentException("Unable to delete system with value null");
		}
		
		try {
			boolean childDeleted = false;
			final String path = getZookeeperPathForDistributionRegion(region) + "/" + NodeNames.NAME_SYSTEMS;
			final List<String> childs = zookeeper.getChildren(path, false);
			
			for(final String childName : childs) {
				final String childPath = path + "/" + childName;
				final byte[] data = zookeeper.getData(childPath, false, null);
				if(system.getStringValue().equals(new String(data))) {
					zookeeper.delete(childPath, -1);
					childDeleted = true;
				}
			}
			
			return childDeleted;
		} catch (KeeperException | InterruptedException e) {
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * Is the zookeeper client connected?
	 * @return
	 */
	public boolean isConnected() {
		if(zookeeper == null) {
			return false;
		}
		
		return zookeeper.getState() == States.CONNECTED;
	}
	
	/**
	 * Returns the name of the cluster
	 * @return
	 */
	public String getClustername() {
		return clustername;
	}
	
	/**
	 * Get the instance name
	 * @return
	 */
	public DistributedInstance getInstancename() {
		return instancename;
	}
}