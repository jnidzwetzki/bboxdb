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
package org.bboxdb.distribution;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.duplicateresolver.DoNothingDuplicateResolver;
import org.bboxdb.storage.sstable.duplicateresolver.TupleDuplicateResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class TupleStoreConfigurationCache {

	/**
	 * The instance
	 */
	private static TupleStoreConfigurationCache instance;
	
	/**
	 * The cache
	 */
	protected final Map<String, TupleStoreConfiguration> cache;
	
	/**
	 * The tuple store name cache
	 */
	protected final LoadingCache<String, Boolean> tupleStoreNameCache = CacheBuilder.newBuilder().expireAfterWrite(15, TimeUnit.SECONDS).build(
			new CacheLoader<String, Boolean>() {

		@Override
		public Boolean load(final String key) throws Exception {
			final TupleStoreName tupleStoreNameObject = new TupleStoreName(key);
			final TupleStoreAdapter tupleStoreAdapter = ZookeeperClientFactory
					.getZookeeperClient().getTupleStoreAdapter();
			
			return tupleStoreAdapter.isTableKnown(tupleStoreNameObject);
		}
				
	});
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleStoreConfigurationCache.class);
	
	static {
		instance = new TupleStoreConfigurationCache();
	}
	
	private TupleStoreConfigurationCache() {
		// private singleton constructor
		cache = new HashMap<>();
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		throw new IllegalArgumentException("Unable to clone a singleton");
	}
	
	/**
	 * Return the instance
	 * @return
	 */
	public static TupleStoreConfigurationCache getInstance() {
		return instance;
	}
	
	/**
	 * 
	 * @param tupleStorename
	 * @return
	 */
	public synchronized DuplicateResolver<Tuple> getDuplicateResolverForTupleStore(final String tupleStorename) {		
		final Optional<TupleStoreConfiguration> configuration = getTupleStoreConfiguration(tupleStorename);
		
		if(! configuration.isPresent()) {
			logger.error("Table {} is not known, using do nothing duplicate resolver", tupleStorename);
			return new DoNothingDuplicateResolver();
		}
		
		return TupleDuplicateResolverFactory.build(configuration.get());
	}

	/**
	 * Get the tuple store configuration
	 * @param tupleStorename
	 * @return 
	 */
	public Optional<TupleStoreConfiguration> getTupleStoreConfiguration(final String tupleStorename) {
		
		if(cache.containsKey(tupleStorename)) {
			return Optional.of(cache.get(tupleStorename));
		}
		
		try {
			final TupleStoreAdapter tupleStoreAdapter = ZookeeperClientFactory
					.getZookeeperClient().getTupleStoreAdapter();
			
			final TupleStoreName tupleStoreNameObject = new TupleStoreName(tupleStorename);

			if(! tupleStoreAdapter.isTableKnown(tupleStoreNameObject)) {
				return Optional.empty();
			}
			
			final TupleStoreConfiguration tupleStoreConfiguration = tupleStoreAdapter.readTuplestoreConfiguration(tupleStoreNameObject);
			cache.put(tupleStorename, tupleStoreConfiguration);
			
			return Optional.of(tupleStoreConfiguration);
		} catch (ZookeeperException e) {
			logger.error("Exception while reading zookeeper data", e);
			return Optional.empty();
		}
	
	}
	
	/**
	 * Is the tuple store known
	 * @param tupleStoreName
	 */
	public synchronized boolean isTupleStoreKnown(final String tupleStoreName) {
		try {
			return tupleStoreNameCache.get(tupleStoreName);
		} catch (ExecutionException e) {
			logger.error("Got exception while checking tuplestore name: " + tupleStoreName, e);
			return false;
		}
	}
	
	/**
	 * Clear the cache
	 */
	public synchronized void clear() {
		cache.clear();
	}

}
