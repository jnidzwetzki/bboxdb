/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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

import java.net.SocketException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.bboxdb.commons.NetworkInterfaceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BBoxDBConfiguration {

	/**
	 *  The directories to store data
	 */
	private List<String> storageDirectories = Arrays.asList("/tmp/bboxdb");

	/**
	 *  Number of entries per memtable
	 */
	private int memtableEntriesMax = 50000;
	
	/**
	 * Size of the memtable in bytes
	 */
	private long memtableSizeMax = 128 * 1024 * 1014;

	/**
	 * Number of memtable flush threads per storage
	 */
	private int memtableFlushThreadsPerStorage = 2;
	
	/**
	 * The classname of the spatial index builder
	 */
	private String storageSpatialIndexBuilder = "org.bboxdb.storage.sstable.spatialindex.rtree.RTreeBuilder";
	
	/**
	 * The classname of the spatial index reader
	 */
	private String storageSpatialIndexReader = "org.bboxdb.storage.sstable.spatialindex.rtree.mmf.RTreeMMFReader";
	
	/**
	 * The checkpoint interval
	 */
	private int storageCheckpointInterval = 60;
	
	/**
	 * The write ahead log
	 */
	private boolean storageWriteAheadLog = false;
	
	/**
	 * The port for client requests
	 */
	private int networkListenPort = 50505;

	/**
	 *  The amount of threads to handle client connections
	 */
	private int networkConnectionThreads = 25;
	
	/**
	 * The name of the cluster
	 */
	private String clustername;
	
	/**
	 * The list of zookeeper nodes 
	 */
	private Collection<String> zookeepernodes;
	
	/**
	 * The local IP address of this node. The default value is set in the constructor.
	 */
	private String localip = null;
	
	/**
	 * The number of entries in the key cache per SSTable
	 */
	private int sstableKeyCacheEntries = 1000;
	
	/**
	 * The port where the performance counter will be exposed
	 */
	private int performanceCounterPort = 10085;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BBoxDBConfiguration.class);

	public BBoxDBConfiguration() {
		try {
			localip = NetworkInterfaceHelper.getFirstNonLoopbackIPv4AsString();
		} catch (SocketException e) {
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

	public int getStorageCheckpointInterval() {
		return storageCheckpointInterval;
	}

	public void setStorageCheckpointInterval(final int storageCheckpointInterval) {
		this.storageCheckpointInterval = storageCheckpointInterval;
	}

	public List<String> getStorageDirectories() {
		return storageDirectories;
	}

	public void setStorageDirectories(final List<String> storageDirectories) {
		this.storageDirectories = storageDirectories;
	}

	public int getMemtableFlushThreadsPerStorage() {
		return memtableFlushThreadsPerStorage;
	}

	public void setMemtableFlushThreadsPerStorage(final int memtableFlushThreadsPerStorage) {
		this.memtableFlushThreadsPerStorage = memtableFlushThreadsPerStorage;
	}

	public String getStorageSpatialIndexBuilder() {
		return storageSpatialIndexBuilder;
	}

	public void setStorageSpatialIndexBuilder(final String storageSpatialIndexBuilder) {
		this.storageSpatialIndexBuilder = storageSpatialIndexBuilder;
	}

	public String getStorageSpatialIndexReader() {
		return storageSpatialIndexReader;
	}

	public void setStorageSpatialIndexReader(final String storageSpatialIndexReader) {
		this.storageSpatialIndexReader = storageSpatialIndexReader;
	}

	public int getSstableKeyCacheEntries() {
		return sstableKeyCacheEntries;
	}

	public void setSstableKeyCacheEntries(final int sstableKeyCacheEntries) {
		this.sstableKeyCacheEntries = sstableKeyCacheEntries;
	}

	public int getPerformanceCounterPort() {
		return performanceCounterPort;
	}

	public void setPerformanceCounterPort(final int performanceCounterPort) {
		this.performanceCounterPort = performanceCounterPort;
	}

	public boolean isStorageWriteAheadLog() {
		return storageWriteAheadLog;
	}

	public void setStorageWriteAheadLog(final boolean storageWriteAheadLog) {
		this.storageWriteAheadLog = storageWriteAheadLog;
	}
	
}
