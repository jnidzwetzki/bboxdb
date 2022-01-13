/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.misc.Const;
import org.bboxdb.query.ContinuousQueryPlan;
import org.bboxdb.query.transformation.EnlargeBoundingBoxByAmountTransformation;
import org.bboxdb.query.transformation.EnlargeBoundingBoxByFactorTransformation;
import org.bboxdb.query.transformation.EnlargeBoundingBoxByWGS84Transformation;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousQueryEnlargementRegisterer implements Watcher {

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
	private final QueryEnlargement queryEnlagement;
	
	/**
	 * The registered query plans
	 */
	private final Map<String, ContinuousQueryPlan> registeredQueryPlans = new ConcurrentHashMap<>();
	
	/**
	 * The instances
	 */
	private final static Map<TupleStoreName, ContinuousQueryEnlargementRegisterer> instances = new ConcurrentHashMap<>();
	
	/**
	 * The node name
	 */
	private final static String NODE_NAME = "/query-";
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ContinuousQueryEnlargementRegisterer.class);


	/**
	 * The factory method - returns singletons for the tuple stores
	 * @param tupleStoreName
	 * @return
	 * @throws StorageManagerException 
	 * @throws ZookeeperException 
	 */
	public static ContinuousQueryEnlargementRegisterer getInstanceFor(final TupleStoreName tupleStoreName) 
			throws StorageManagerException, ZookeeperException {
		
		if(instances.containsKey(tupleStoreName)) {
			return instances.get(tupleStoreName);
		}
		
		final TupleStoreAdapter tupleStoreAdapter = ZookeeperClientFactory.getZookeeperClient().getTupleStoreAdapter();
				
		if(! tupleStoreAdapter.isTableKnown(tupleStoreName)) {
			throw new StorageManagerException("Table: " + tupleStoreName.getFullname() + " is unknown");
		}
		
		final ContinuousQueryEnlargementRegisterer registerer = new ContinuousQueryEnlargementRegisterer(tupleStoreName);
		instances.put(tupleStoreName, registerer);
		
		return registerer;
	}
	
	
	private ContinuousQueryEnlargementRegisterer(final TupleStoreName tupleStoreName) {		
		this.zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		this.queryEnlagement = new QueryEnlargement();
		
		final TupleStoreAdapter tupleStoreAdapter = zookeeperClient.getTupleStoreAdapter();
		final String tablePath = tupleStoreAdapter.getTablePath(tupleStoreName);

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
	 * Get the max enlargement factor for a table
	 * 
	 * @param distributionGroup
	 * @param table
	 * @return
	 */
	public QueryEnlargement getEnlagementForTable() {		
		readMaxEnlargementFromZookeeper();
		return queryEnlagement;
	}

	/**
	 * Get the children values a 
	 * @param basePath
	 * @param chrindren
	 * @return
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	protected List<Double> getChildrenValues(final String basePath, final List<String> chrindren) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		final List<Double> resultList = new ArrayList<Double>();
		
		for(final String child: chrindren) {
			final String fullpath = basePath + "/" + child;
			
			try {
				final String value = zookeeperClient.getData(fullpath);
				final Optional<Double> doubleValue = MathUtil.tryParseDouble(value);
				
				if(! doubleValue.isPresent()) {
					logger.error("Unable to parse {} as double", value);
				} else {
					resultList.add(doubleValue.get());
				}
			} catch(ZookeeperNotFoundException e) {
				// Node delete during read, ignore
			}
		}
		
		return resultList;
	}
	
	/**
	 * Update the enlargement data
	 */
	private void readMaxEnlargementFromZookeeper() {
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
					.orElse(1);
			
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
		readMaxEnlargementFromZookeeper();
	}
	
	/**
	 * Unregister the given query
	 * @param queryUUID 
	 * @throws BBoxDBException 
	 */
	public void unregisterOldQuery(final String queryUUID) {
		try {
			registeredQueryPlans.remove(queryUUID);
			updateRegistration();
		} catch (BBoxDBException e) {
			logger.error("Got Zookeeper exception", e);
		}
	}
	
	/**
	 * Unregister all registered queries
	 * @throws BBoxDBException 
	 */
	public void unregisterAllQueries() throws BBoxDBException {
		registeredQueryPlans.clear();
		updateRegistration();
	}
	
	
	/**
	 * Register the query enlargement
	 * 
	 * @param queryPlan
	 * @return
	 * @throws BBoxDBException 
	 */
	public void registerQueryEnlargement(final ContinuousQueryPlan queryPlan) throws BBoxDBException {
		registeredQueryPlans.put(queryPlan.getQueryUUID(), queryPlan);
		updateRegistration();
	}


	/**
	 * Update the registration in ZooKeeper
	 * @throws BBoxDBException 
	 */
	private void updateRegistration() throws BBoxDBException {

		try {
			double maxEnlargementAbsolute = 0;
			double maxEnlargementFactor = 1;
			double maxEnlargementLatMeter = 0;
			double maxEnlargementLonMeter = 0;
			
			for(final ContinuousQueryPlan queryPlan : registeredQueryPlans.values()) {
				
				final double maxEnlargementAbsoluteQuery = queryPlan.getStreamTransformation()
						.stream()
						.filter(t -> t instanceof EnlargeBoundingBoxByAmountTransformation)
						.map(t -> (EnlargeBoundingBoxByAmountTransformation) t)
						.mapToDouble(t -> t.getAmount())
						.sum();
				maxEnlargementAbsolute = Math.max(maxEnlargementAbsolute, maxEnlargementAbsoluteQuery);
					
				final double maxEnlargementFactorQuery = queryPlan.getStreamTransformation()
						.stream()
						.filter(t -> t instanceof EnlargeBoundingBoxByFactorTransformation)
						.map(t -> (EnlargeBoundingBoxByFactorTransformation) t)
						.mapToDouble(t -> t.getFactor())
						.reduce(1, (a, b) -> a * b);
				maxEnlargementFactor = Math.max(maxEnlargementFactor, maxEnlargementFactorQuery);

				final double maxEnlargementLatMeterQuery = queryPlan.getStreamTransformation()
						.stream()
						.filter(t -> t instanceof EnlargeBoundingBoxByWGS84Transformation)
						.map(t -> (EnlargeBoundingBoxByWGS84Transformation) t)
						.mapToDouble(t -> t.getMeterLat())
						.sum();
				maxEnlargementLatMeter = Math.max(maxEnlargementLatMeter, maxEnlargementLatMeterQuery);
				
				final double maxEnlargementLonMeterQuery = queryPlan.getStreamTransformation()
						.stream()
						.filter(t -> t instanceof EnlargeBoundingBoxByWGS84Transformation)
						.map(t -> (EnlargeBoundingBoxByWGS84Transformation) t)
						.mapToDouble(t -> t.getMeterLon())
						.sum();
				maxEnlargementLonMeter = Math.max(maxEnlargementLonMeter, maxEnlargementLonMeterQuery);				
			}
	
			logger.debug("Register query enlargement (absoluteEnlargement={}, enlagementFactor={}, "
					+ "enlargementMeterLat={}, enlargementMeterLon={})", 
					maxEnlargementAbsolute, maxEnlargementFactor, maxEnlargementLatMeter, maxEnlargementLonMeter);
			
			final byte[] absoluteEnlargementBytes = Double.toString(maxEnlargementAbsolute).getBytes(Const.DEFAULT_CHARSET);
			final byte[] factorEnlargementBytes = Double.toString(maxEnlargementFactor).getBytes(Const.DEFAULT_CHARSET);
			final byte[] enlargementLatBytes = Double.toString(maxEnlargementLatMeter).getBytes(Const.DEFAULT_CHARSET);
			final byte[] enlargementLonBytes = Double.toString(maxEnlargementLonMeter).getBytes(Const.DEFAULT_CHARSET);

			final String createdAbsoluteEnlargementPath = zookeeperClient.createEphemeralSequencialNode(pathEnlargementAbsolute + NODE_NAME, absoluteEnlargementBytes);
			if(createdEnlargementAbsolutePath.isPresent()) {
				zookeeperClient.deleteNodesRecursive(createdEnlargementAbsolutePath.get());
				createdEnlargementAbsolutePath = Optional.empty();
			}
			createdEnlargementAbsolutePath = Optional.of(createdAbsoluteEnlargementPath);
			

			final String cratedFactorEnlargementPath = zookeeperClient.createEphemeralSequencialNode(pathEnlargementFactor + NODE_NAME, factorEnlargementBytes);
			if(createdEnlargementFactorPath.isPresent()) {
				zookeeperClient.deleteNodesRecursive(createdEnlargementFactorPath.get());
				createdEnlargementFactorPath = Optional.empty();
			}
			createdEnlargementFactorPath = Optional.of(cratedFactorEnlargementPath);
			
			final String cratedEnlragementLatPath = zookeeperClient.createEphemeralSequencialNode(pathEnlargementMeterLat + NODE_NAME, enlargementLatBytes);
			if(createdEnlargementMeterLatPath.isPresent()) {
				zookeeperClient.deleteNodesRecursive(createdEnlargementMeterLatPath.get());
				createdEnlargementMeterLatPath = Optional.empty();
			}
			createdEnlargementMeterLatPath = Optional.of(cratedEnlragementLatPath);
			
			final String cratedEnlragementLonPath = zookeeperClient.createEphemeralSequencialNode(pathEnlargementMeterLon + NODE_NAME, enlargementLonBytes);
			if(createdEnlargementMeterLonPath.isPresent()) {
				zookeeperClient.deleteNodesRecursive(createdEnlargementMeterLonPath.get());
				createdEnlargementMeterLonPath = Optional.empty();
			}
			createdEnlargementMeterLonPath = Optional.of(cratedEnlragementLonPath);
		
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}
		
	}
	
}