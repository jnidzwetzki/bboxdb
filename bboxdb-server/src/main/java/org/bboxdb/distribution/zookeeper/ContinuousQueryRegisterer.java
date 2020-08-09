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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
	 * The created enlargement absolute path
	 */
	private Optional<String> createdEnlargementAbsolutePath = Optional.empty();
	
	/**
	 * The factor enlargement path
	 */
	private final String pathEnlargementFactor;
	
	/**
	 * The created enlargement factor path
	 */
	private Optional<String> createdEnlargementFactorPath = Optional.empty();
	
	/**
	 * The factor enlargement meter lat path
	 */
	private final String pathEnlargementMeterLat;
	
	/**
	 * The created enlargement meter lat path
	 */
	private Optional<String> createdEnlargementMeterLatPath = Optional.empty();

	/**
	 * The query enlargement meter lon the given table
	 */
	private final String pathEnlargementMeterLon;
	
	/**
	 * The created enlargement meter lon path
	 */
	private Optional<String> createdEnlargementMeterLonPath = Optional.empty();
		
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

		pathEnlargementAbsolute = tablePath + "/queries/absolute-enlargement"; 	
		pathEnlargementFactor = tablePath + "/queries/factor-enlargement";
		pathEnlargementMeterLat = tablePath + "/queries/meter-lan-enlargement"; 	
		pathEnlargementMeterLon = tablePath + "/queries/meter-lon-enlargement";
		
		try {
			zookeeperClient.createDirectoryStructureRecursive(pathEnlargementAbsolute);
			zookeeperClient.createDirectoryStructureRecursive(pathEnlargementFactor);
			zookeeperClient.createDirectoryStructureRecursive(pathEnlargementMeterLat);
			zookeeperClient.createDirectoryStructureRecursive(pathEnlargementMeterLon);
		} catch (ZookeeperException e) {
			logger.error("Got exception while accessing zookeeper", e);
		}
	}

	/**
	 * Register a continuous query enlargement of the given table
	 * @param enlagementFactor
	 * @param distributionGroup
	 * @param table
	 * @param enlargement
	 * 
	 * @throws ZookeeperException 
	 */
	public void updateQueryOnTable(final double absoluteEnlargement, final double enlagementFactor, 
			final double enlargementMeterLat, final double enlargementMeterLon) throws ZookeeperException {
		
		unregisterOldQuery();
		
		final String queryNodeEnlargementAbsolute = pathEnlargementAbsolute + "/query-";
		final String queryNodeEnlargementFactor = pathEnlargementFactor + "/query-";
		final String queryNodeEnlargementMeterLat = pathEnlargementMeterLat + "/query-";
		final String queryNodeEnlargementMeterLon = pathEnlargementMeterLon + "/query-";
		
		final byte[] absoluteEnlargementBytes = Double.toString(absoluteEnlargement).getBytes();
		final byte[] factorEnlargementBytes = Double.toString(enlagementFactor).getBytes();
		final byte[] enlargementLatBytes = Double.toString(enlargementMeterLat).getBytes();
		final byte[] enlargementLonBytes = Double.toString(enlargementMeterLon).getBytes();

		final String createdAbsoluteEnlargementPath = zookeeperClient.craateEphemeralSequencialNode(queryNodeEnlargementAbsolute, absoluteEnlargementBytes);
		createdEnlargementAbsolutePath = Optional.of(createdAbsoluteEnlargementPath);
		
		final String cratedFactorEnlargementPath = zookeeperClient.craateEphemeralSequencialNode(queryNodeEnlargementFactor, factorEnlargementBytes);
		createdEnlargementFactorPath = Optional.of(cratedFactorEnlargementPath);
		
		final String cratedEnlragementLatPath = zookeeperClient.craateEphemeralSequencialNode(queryNodeEnlargementMeterLat, enlargementLatBytes);
		createdEnlargementMeterLatPath = Optional.of(cratedEnlragementLatPath);
		
		final String cratedEnlragementLonPath = zookeeperClient.craateEphemeralSequencialNode(queryNodeEnlargementMeterLon, enlargementLonBytes);
		createdEnlargementMeterLonPath = Optional.of(cratedEnlragementLonPath);
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
	 * Get the children values a 
	 * @param basePath
	 * @param chrindren
	 * @return
	 * @throws ZookeeperException 
	 */
	protected List<Double> getChildrenValues(final String basePath, final List<String> chrindren) throws ZookeeperException {
		
		final List<Double> resultList = new ArrayList<Double>();
		
		for(final String child: chrindren) {
			final String fullpath = basePath + "/" + child;
			final String value = zookeeperClient.getData(fullpath);
			final Optional<Double> doubleValue = MathUtil.tryParseDouble(value);
			
			if(! doubleValue.isPresent()) {
				logger.error("Unable to parse {} as double", value);
			} else {
				resultList.add(doubleValue.get());
			}
		}
		
		return resultList;
	}
	
	/**
	 * Update the enlargement data
	 */
	private void updateQueryEnlargement() {
		try {
			final List<String> absoluteEnlargements = zookeeperClient.getChildren(pathEnlargementAbsolute, this);
			final List<Double> doubleAbsoluteEnlargements = getChildrenValues(pathEnlargementAbsolute, absoluteEnlargements);
			
			final double maxAbsoluteEnlargement = doubleAbsoluteEnlargements
					.stream()
					.mapToDouble(s -> s)
					.max()
					.orElse(0);
			
			queryEnlagement.setMaxAbsoluteEnlargement(maxAbsoluteEnlargement);
			
			final List<String> factorEnlargements = zookeeperClient.getChildren(pathEnlargementFactor, this);
			final List<Double> doubleFactorEnlargements = getChildrenValues(pathEnlargementFactor, factorEnlargements);

			final double maxFactorEnlargement = doubleFactorEnlargements
					.stream()
					.mapToDouble(s -> s)
					.max()
					.orElse(0);
			
			queryEnlagement.setMaxEnlargementFactor(maxFactorEnlargement);
			
			final List<String> latEnlargements = zookeeperClient.getChildren(pathEnlargementMeterLat, this);
			final List<Double> doublelatEnlargements = getChildrenValues(pathEnlargementMeterLat, latEnlargements);

			final double maxLatEnlargement = doublelatEnlargements
					.stream()
					.mapToDouble(s -> s)
					.max()
					.orElse(0);
			
			queryEnlagement.setMaxEnlargementLat(maxLatEnlargement);
			
			final List<String> lonEnlargements = zookeeperClient.getChildren(pathEnlargementMeterLon, this);
			final List<Double> doublelonEnlargements = getChildrenValues(pathEnlargementMeterLon, lonEnlargements);

			final double maxLonEnlargement = doublelonEnlargements
					.stream()
					.mapToDouble(s -> s)
					.max()
					.orElse(0);
			
			queryEnlagement.setMaxEnlargementLon(maxLonEnlargement);
			
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
	
	/**
	 * Unregister all queries
	 */
	public void unregisterOldQuery() {
		try {
			if(createdEnlargementFactorPath.isPresent()) {
				zookeeperClient.deleteNodesRecursive(createdEnlargementFactorPath.get());
				createdEnlargementFactorPath = Optional.empty();
			}
			
			if(createdEnlargementAbsolutePath.isPresent()) {
				zookeeperClient.deleteNodesRecursive(createdEnlargementAbsolutePath.get());
				createdEnlargementAbsolutePath = Optional.empty();
			}
			
			if(createdEnlargementMeterLatPath.isPresent()) {
				zookeeperClient.deleteNodesRecursive(createdEnlargementMeterLatPath.get());
				createdEnlargementMeterLatPath = Optional.empty();
			}
			
			if(createdEnlargementMeterLonPath.isPresent()) {
				zookeeperClient.deleteNodesRecursive(createdEnlargementMeterLonPath.get());
				createdEnlargementMeterLonPath = Optional.empty();
			}
		} catch (ZookeeperException e) {
			logger.error("Got an exception while deleting enlargement", e);
		}
	}
	
}