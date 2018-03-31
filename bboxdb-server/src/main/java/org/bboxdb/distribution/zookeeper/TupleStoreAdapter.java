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
package org.bboxdb.distribution.zookeeper;

import java.util.List;

import org.apache.zookeeper.Watcher;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;

public class TupleStoreAdapter {
	
	/**
	 * The spatial index writer
	 */
	public static final String ZOOKEEPER_SPATIAL_INDEX_WRITER = "sindex_writer";
	
	/**
	 * The spatial index reader
	 */
	public static final String ZOOKEEPER_SPATIAL_INDEX_READER = "sindex_reader";

	/**
	 * The duplicate allowed
	 */
	public static final String ZOOKEEPER_DUPLICATES_ALLOWED = "duplicate_allowed";

	/**
	 * The duplicate versions
	 */
	public static final String ZOOKEEPER_DUPLICATES_VERSIONS = "duplicate_versions";
	
	/**
	 * The duplicate ttl
	 */
	public static final String ZOOKEEPER_DUPLICATES_TTL = "duplicate_ttl";

	/**
	 * The zookeeper client
	 */
	private final ZookeeperClient zookeeperClient;
	
	/**
	 * The distribution group adapter
	 */
	private final DistributionGroupAdapter distributionGroupAdapter;

	public TupleStoreAdapter(final ZookeeperClient zookeeperClient) {
		this.zookeeperClient = zookeeperClient;
		this.distributionGroupAdapter = new DistributionGroupAdapter(zookeeperClient);
	}
	
	/**
	 * Write the tuple store configuration
	 * @param tupleStoreName
	 * @param tupleStoreConfiguration
	 * @throws ZookeeperException 
	 */
	public void writeTuplestoreConfiguration(final TupleStoreName tupleStoreName, 
			final TupleStoreConfiguration tupleStoreConfiguration) throws ZookeeperException {
		
		final String tablePath = getTablePath(tupleStoreName);
		zookeeperClient.createDirectoryStructureRecursive(tablePath);
				
		final String spatialIndexReader = tupleStoreConfiguration.getSpatialIndexReader();
		zookeeperClient.createPersistentNode(getIndexReaderPath(tupleStoreName), 
				spatialIndexReader.getBytes());
		
		final String spatialIndexWriter = tupleStoreConfiguration.getSpatialIndexWriter();
		zookeeperClient.createPersistentNode(getIndexWriterPath(tupleStoreName), 
				spatialIndexWriter.getBytes());
		
		final boolean allowDuplicates = tupleStoreConfiguration.isAllowDuplicates();
		final String allowDuplicatesString = Boolean.toString(allowDuplicates);
		zookeeperClient.createPersistentNode(getDuplicatesAllowedPath(tupleStoreName), 
				allowDuplicatesString.getBytes());
		
		final long ttl = tupleStoreConfiguration.getTTL();
		final String ttlString = Long.toString(ttl);
		zookeeperClient.createPersistentNode(getDuplicatesTTLPath(tupleStoreName), 
				ttlString.getBytes());
		
		final int versions = tupleStoreConfiguration.getVersions();
		final String versionsString = Integer.toString(versions);
		zookeeperClient.createPersistentNode(getDuplicateVersionsPath(tupleStoreName), 
				versionsString.getBytes());
		
		NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, tablePath);
		
		final String allTablesPath = getAllTablesPath(tupleStoreName.getDistributionGroup());
		NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, allTablesPath);
	}

	/**
	 * Create the tuple store dir
	 * 
	 * @param tupleStoreName
	 * @throws ZookeeperException
	 */
	public void createTupleStoreConfigNode(final String distributionGroup) throws ZookeeperException {
		final String allTablesPath = getAllTablesPath(distributionGroup);
		NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, allTablesPath);
		zookeeperClient.createDirectoryStructureRecursive(allTablesPath);
		NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, allTablesPath);
	}

	/**
	 * Get the table path
	 * @param tupleStoreName
	 * @return
	 */
	private String getTablePath(final TupleStoreName tupleStoreName) {
		final String distributionGroup = tupleStoreName.getDistributionGroup();
		return getAllTablesPath(distributionGroup) + "/" + tupleStoreName.getFullnameWithoutPrefix();
	}
	
	/**
	 * Get the path for all tables
	 * @param distributionGroup
	 * @return
	 */
	public String getAllTablesPath(final String distributionGroup) {
		return distributionGroupAdapter.getDistributionGroupPath(distributionGroup) 
				+ "/" + ZookeeperNodeNames.NAME_TABLES;
	}
	
	/**
	 * Get all known tables
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	public List<String> getAllTables(final String distributionGroup) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		return getAllTables(distributionGroup, null);
	}
	
	/**
	 * Get all known tables
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	public List<String> getAllTables(final String distributionGroup, final Watcher watcher) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		final String allTablesPath = getAllTablesPath(distributionGroup);
		NodeMutationHelper.getNodeMutationVersion(zookeeperClient, allTablesPath, watcher);
				
		final List<String> children = zookeeperClient.getChildren(allTablesPath);
		children.removeIf(c -> c.endsWith(ZookeeperNodeNames.NAME_NODE_VERSION));
		
		return children;
	}
	
	/**
	 * Read the tuple store name
	 * @param tupleStoreName
	 * @throws ZookeeperException 
	 */
	public TupleStoreConfiguration readTuplestoreConfiguration(final TupleStoreName tupleStoreName) 
			throws ZookeeperException {
		
		final TupleStoreConfiguration tupleStoreConfiguration = new TupleStoreConfiguration();
	
		try {
			final String spatialIndexReader = 
					zookeeperClient.readPathAndReturnString(getIndexReaderPath(tupleStoreName));
			tupleStoreConfiguration.setSpatialIndexReader(spatialIndexReader);
			
			final String spatialIndexWriter = 
					zookeeperClient.readPathAndReturnString(getIndexWriterPath(tupleStoreName));
			tupleStoreConfiguration.setSpatialIndexWriter(spatialIndexWriter);
			
			final String duplicatesAllowed = 
					zookeeperClient.readPathAndReturnString(getDuplicatesAllowedPath(tupleStoreName));
			final boolean duplicatesAllowedBoolean = Boolean.parseBoolean(duplicatesAllowed);
			tupleStoreConfiguration.setAllowDuplicates(duplicatesAllowedBoolean);
			
			final String duplicatesTTL = 
					zookeeperClient.readPathAndReturnString(getDuplicatesTTLPath(tupleStoreName));
			final Integer ttlInterger = Integer.parseInt(duplicatesTTL);
			tupleStoreConfiguration.setTtl(ttlInterger);
			
			final String duplicateVersions = 
					zookeeperClient.readPathAndReturnString(getDuplicateVersionsPath(tupleStoreName));
			
			final Integer duplicateVersionsInteger = Integer.parseInt(duplicateVersions);
			tupleStoreConfiguration.setVersions(duplicateVersionsInteger);
		} catch (ZookeeperNotFoundException | NumberFormatException e) {
			throw new ZookeeperException(e);
		}
		
		return tupleStoreConfiguration;
	}
	
	/**
	 * Is the table known?
	 * @return
	 * @throws ZookeeperException
	 */
	public boolean isTableKnown(final TupleStoreName tupleStoreName) throws ZookeeperException {
		final String tablePath = getTablePath(tupleStoreName);
		return zookeeperClient.exists(tablePath);
	}
	
	/**
	 * Delete the given tuple store name config
	 * @param tupleStoreName
	 * @throws ZookeeperException 
	 */
	public void deleteTable(final TupleStoreName tupleStoreName) throws ZookeeperException {
		final String tablePath = getTablePath(tupleStoreName);
		zookeeperClient.deleteNodesRecursive(tablePath);
		final String allTablesPath = getAllTablesPath(tupleStoreName.getDistributionGroup());	
		
		NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, allTablesPath);
	}

	/**
	 * The duplicate versions path
	 * @param tupleStoreName
	 * @return
	 */
	private String getDuplicateVersionsPath(final TupleStoreName tupleStoreName) {
		final String tablePath = getTablePath(tupleStoreName);
		return tablePath + "/" + ZOOKEEPER_DUPLICATES_VERSIONS;
	}

	/**
	 * The duplicates TTL path
	 * @param tupleStoreName
	 * @return
	 */
	private String getDuplicatesTTLPath(final TupleStoreName tupleStoreName) {
		final String tablePath = getTablePath(tupleStoreName);
		return tablePath + "/" + ZOOKEEPER_DUPLICATES_TTL;
	}

	/**
	 * The duplicates allowed path
	 * @param tupleStoreName
	 * @return
	 */
	private String getDuplicatesAllowedPath(final TupleStoreName tupleStoreName) {
		final String tablePath = getTablePath(tupleStoreName);
		return tablePath + "/" + ZOOKEEPER_DUPLICATES_ALLOWED;
	}

	/**
	 * The index writer path
	 * @param tupleStoreName
	 * @return
	 */
	private String getIndexWriterPath(final TupleStoreName tupleStoreName) {
		final String tablePath = getTablePath(tupleStoreName);
		return tablePath + "/" + ZOOKEEPER_SPATIAL_INDEX_WRITER;
	}

	/**
	 * The index reader path
	 * @param tupleStoreName
	 * @return
	 */
	private String getIndexReaderPath(final TupleStoreName tupleStoreName) {
		final String tablePath = getTablePath(tupleStoreName);
		return tablePath + "/" + ZOOKEEPER_SPATIAL_INDEX_READER;
	}
}
