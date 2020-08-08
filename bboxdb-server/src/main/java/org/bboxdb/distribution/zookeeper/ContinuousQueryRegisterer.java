/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.bboxdb.commons.MathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousQueryRegisterer implements Watcher {

	/**
	 * The zookeeper client
	 */
	private final ZookeeperClient zookeeperClient;
	
	/**
	 * The absolute enlargement path
	 */
	private final String pathEnlargementAbsolute;
	
	/**
	 * The factor enlargement path
	 */
	private final String pathEnlargementFactor;
	
	/**
	 * The query enlargement for the given table
	 */
	private final QueryEnlagement queryEnlagement;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ContinuousQueryRegisterer.class);

	
	public ContinuousQueryRegisterer(final String distributionGroup, final String table) {
		this(distributionGroup, table, ZookeeperClientFactory.getZookeeperClient());
	}
	
	public ContinuousQueryRegisterer(final String distributionGroup, 
			final String table, final ZookeeperClient zookeeperClient) {
		
		this.zookeeperClient = zookeeperClient;
		this.queryEnlagement = new QueryEnlagement();
		
		final TupleStoreAdapter tupleStoreAdapter = zookeeperClient.getTupleStoreAdapter();
		final String tablePath = tupleStoreAdapter.getTablePath(distributionGroup, table);

		pathEnlargementAbsolute = tablePath + "/queries-absolute-enlargement"; 	
		pathEnlargementFactor = tablePath + "/queries-factor-enlargement";
		
		try {
			zookeeperClient.createDirectoryStructureRecursive(pathEnlargementAbsolute);
			zookeeperClient.createDirectoryStructureRecursive(pathEnlargementFactor);
		} catch (ZookeeperException e) {
			logger.error("Got exception while accessing zookeeper", e);
		}
		
	}

	/**
	 * Register a continuous query enlargement of the given table
	 * 
	 * @param distributionGroup
	 * @param table
	 * @param enlagementFactor
	 * @param enlargement
	 * @throws ZookeeperException 
	 */
	public void registerQueryOnTable(final double enlagementFactor, final double absoluteEnlargement) throws ZookeeperException {
		final String queryNodeEnlargementAbsolute = pathEnlargementAbsolute + "/query";
		final String queryNodeEnlargementFactor = pathEnlargementFactor + "/query";
		
		final byte[] absoluteEnlargementBytes = Double.toString(absoluteEnlargement).getBytes();
		final byte[] factorEnlargementBytes = Double.toString(enlagementFactor).getBytes();

		zookeeperClient.craateEphemeralSequencialNode(queryNodeEnlargementAbsolute, absoluteEnlargementBytes);
		zookeeperClient.craateEphemeralSequencialNode(queryNodeEnlargementFactor, factorEnlargementBytes);
	}
	
	/**
	 * Get the max enlargement factor for a table
	 * 
	 * @param distributionGroup
	 * @param table
	 * @return
	 */
	public QueryEnlagement getMaxEnlagementFactorForTable() {		
		updateQueryEnlargement();
		return queryEnlagement;
	}

	/**
	 * Update the enlargement data
	 */
	private void updateQueryEnlargement() {
		try {
			final List<String> absoluteEnlargements = zookeeperClient.getChildren(pathEnlargementAbsolute, this);
			
			final double maxAbsoluteEnlargement = absoluteEnlargements.stream()
				.mapToDouble(s -> MathUtil.tryParseDouble(s).get())
				.max().orElse(0);
			
			queryEnlagement.setMaxAbsoluteEnlargement(maxAbsoluteEnlargement);
			
			final List<String> factorEnlargements = zookeeperClient.getChildren(pathEnlargementFactor, this);
			
			final double maxFactorEnlargement = factorEnlargements.stream()
					.mapToDouble(s -> MathUtil.tryParseDouble(s).get())
					.max().orElse(0);
			
			queryEnlagement.setMaxEnlargementFactor(maxFactorEnlargement);
			
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			logger.error("Got an exception while updating enlargement", e);
		} 
	}

	
	/**
	 * Callbacks for changed elements
	 */
	@Override
	public void process(final WatchedEvent watchedEvent) {
		updateQueryEnlargement();
	}
	
}