package de.fernunihagen.dna.jkn.scalephant;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScalephantConfiguration {
	
	/**
	 * The root directory of the application
	 */
	protected String rootDirectory = "/tmp/scalephant_test";
	
	/**
	 *  The directory to store data
	 */
	protected String dataDirectory = "/tmp/scalephant_test/data";
	
	/**
	 * The commit log dir
	 */
	protected String commitlogDir = "/tmp/scalephant_test/commitlog";
	
	/**
	 *  Number of entries per memtable
	 */
	protected int memtableEntriesMax = 10000;
	
	/**
	 * Size of the memtable in KB
	 */
	protected int memtableSizeMax = 128 * 1024;
	
	/**
	 * Start compact thread (can be disabled for tests)
	 */
	protected boolean storageRunCompactThread = true;
	
	/**
	 * Start flush thread (can be disabled for tests - all data stays in memory)
	 */
	protected boolean storageRunMemtableFlushThread = true;
	
	/**
	 * The classname of the spatial indexer
	 */
	protected String storageSpatialIndexerFactory = "none";
	
	/**
	 * The snapshot interval
	 */
	protected int storageSnapshotInterval = 1800;
	
	/**
	 * The port for client requests
	 */
	protected int networkListenPort = 50505;

	/**
	 *  The amount of threads to handle client connections
	 */
	protected int networkConnectionThreads = 10;
	
	/**
	 * The name of the cluster
	 */
	protected String clustername;
	
	/**
	 * The list of zookeeper nodes 
	 */
	protected Collection<String> zookeepernodes;
	
	/**
	 * The local IP address of this node. The default value is set in the constructor.
	 */
	protected String localip = null;
	
	/**
	 * The sstable split strategy
	 */
	protected String regionSplitStrategy = "de.fernunihagen.dna.jkn.scalephant.distribution.regionsplit.SimpleSplitStrategy";

	/**
	 * The resource placement strategy
	 */
	protected String resourcePlacementStrategy = "de.fernunihagen.dna.jkn.scalephant.distribution.placement.RandomResourcePlacementStrategy";
	
	/**
	 * The maximum number of entries per SSTable
	 */
	protected int sstableMaxEntries = 1000;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ScalephantConfiguration.class);

	public ScalephantConfiguration() {
		try {
			localip = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			logger.warn("Unable to determine the local IP adress of this node, please specify 'localip' in the configuration", e);
		}
	}
	
	public String getRootDirectory() {
		return rootDirectory;
	}
	
	public void setRootDirectory(final String rootDirectory) {
		this.rootDirectory = rootDirectory;
	}
	
	public String getDataDirectory() {
		return dataDirectory;
	}
	
	public void setDataDirectory(final String dataDirectory) {
		this.dataDirectory = dataDirectory;
	}

	public String getCommitlogDir() {
		return commitlogDir;
	}

	public void setCommitlogDir(final String commitlogDir) {
		this.commitlogDir = commitlogDir;
	}

	public int getMemtableEntriesMax() {
		return memtableEntriesMax;
	}

	public void setMemtableEntriesMax(final int memtableEntriesMax) {
		this.memtableEntriesMax = memtableEntriesMax;
	}

	public int getMemtableSizeMax() {
		return memtableSizeMax;
	}

	public void setMemtableSizeMax(final int memtableSizeMax) {
		this.memtableSizeMax = memtableSizeMax;
	}

	public boolean isStorageRunCompactThread() {
		return storageRunCompactThread;
	}

	public void setStorageRunCompactThread(final boolean storageRunCompactThread) {
		this.storageRunCompactThread = storageRunCompactThread;
	}

	public boolean isStorageRunMemtableFlushThread() {
		return storageRunMemtableFlushThread;
	}

	public void setStorageRunMemtableFlushThread(
			final boolean storageRunMemtableFlushThread) {
		this.storageRunMemtableFlushThread = storageRunMemtableFlushThread;
	}

	public int getNetworkListenPort() {
		return networkListenPort;
	}

	public void setNetworkListenPort(final int networkListenPort) {
		this.networkListenPort = networkListenPort;
	}

	public int getNetworkConnectionThreads() {
		return networkConnectionThreads;
	}

	public void setNetworkConnectionThreads(final int networkConnectionThreads) {
		this.networkConnectionThreads = networkConnectionThreads;
	}

	public String getStorageSpatialIndexerFactory() {
		return storageSpatialIndexerFactory;
	}

	public void setStorageSpatialIndexerFactory(String storageSpatialIndexerFactory) {
		this.storageSpatialIndexerFactory = storageSpatialIndexerFactory;
	}

	public String getClustername() {
		return clustername;
	}

	public void setClustername(final String clustername) {
		this.clustername = clustername;
	}

	public Collection<String> getZookeepernodes() {
		return zookeepernodes;
	}

	public void setZookeepernodes(final Collection<String> zookeepernodes) {
		this.zookeepernodes = zookeepernodes;
	}

	public String getLocalip() {
		return localip;
	}

	public void setLocalip(final String localip) {
		this.localip = localip;
	}

	public int getSstableMaxEntries() {
		return sstableMaxEntries;
	}

	public void setSstableMaxEntries(final int sstableMaxEntries) {
		this.sstableMaxEntries = sstableMaxEntries;
	}

	public String getRegionSplitStrategy() {
		return regionSplitStrategy;
	}

	public void setRegionSplitStrategy(final String regionSplitStrategy) {
		this.regionSplitStrategy = regionSplitStrategy;
	}

	public String getResourcePlacementStrategy() {
		return resourcePlacementStrategy;
	}

	public void setResourcePlacementStrategy(final String resourcePlacementStrategy) {
		this.resourcePlacementStrategy = resourcePlacementStrategy;
	}

	public int getStorageSnapshotInterval() {
		return storageSnapshotInterval;
	}

	public void setStorageSnapshotInterval(final int storageSnapshotInterval) {
		this.storageSnapshotInterval = storageSnapshotInterval;
	}
}
