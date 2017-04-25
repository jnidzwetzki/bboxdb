/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
package org.bboxdb.misc;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BBoxDBConfiguration {

	/**
	 *  The directories to store data
	 */
	protected List<String> storageDirectories = Arrays.asList("/tmp/bboxdb");

	/**
	 *  Number of entries per memtable
	 */
	protected int memtableEntriesMax = 10000;
	
	/**
	 * Size of the memtable in bytes
	 */
	protected long memtableSizeMax = 128 * 1024 * 1014;
	
	/**
	 * The maximum size of a region in bytes
	 */
	protected long regionMaxSize = 256 * 1024 * 1014;
	
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
	protected String storageSpatialIndexerFactory = "org.bboxdb.storage.sstable.spatialindex.rtree.RTreeSpatialIndexStrategy";
	
	/**
	 * The checkpoint interval
	 */
	protected int storageCheckpointInterval = 1800;
	
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
	protected String regionSplitStrategy = "org.bboxdb.distribution.regionsplit.WeightBasedSplitStrategy";

	/**
	 * The resource placement strategy
	 */
	protected String resourcePlacementStrategy = "org.bboxdb.distribution.placement.RandomResourcePlacementStrategy";

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BBoxDBConfiguration.class);

	public BBoxDBConfiguration() {
		try {
			localip = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			logger.warn("Unable to determine the local IP adress of this node, please specify 'localip' in the configuration", e);
		}
	}

	public int getMemtableEntriesMax() {
		return memtableEntriesMax;
	}

	public void setMemtableEntriesMax(final int memtableEntriesMax) {
		this.memtableEntriesMax = memtableEntriesMax;
	}

	public long getMemtableSizeMax() {
		return memtableSizeMax;
	}

	public void setMemtableSizeMax(final long memtableSizeMax) {
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

	public int getStorageCheckpointInterval() {
		return storageCheckpointInterval;
	}

	public void setStorageCheckpointInterval(final int storageCheckpointInterval) {
		this.storageCheckpointInterval = storageCheckpointInterval;
	}

	public long getRegionMaxSize() {
		return regionMaxSize;
	}

	public void setRegionMaxSize(final long regionMaxSize) {
		this.regionMaxSize = regionMaxSize;
	}

	public List<String> getStorageDirectories() {
		return storageDirectories;
	}

	public void setStorageDirectories(final List<String> storageDirectories) {
		this.storageDirectories = storageDirectories;
	}
	
}
