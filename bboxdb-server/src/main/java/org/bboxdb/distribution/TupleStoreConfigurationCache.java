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
package org.bboxdb.distribution;

import java.util.HashMap;
import java.util.Map;

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

public class TupleStoreConfigurationCache {

	/**
	 * The instance
	 */
	private static TupleStoreConfigurationCache instance;
	
	/**
	 * The cache
	 */
	protected final Map<String, DuplicateResolver<Tuple>> cache;
	
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
	
	public synchronized DuplicateResolver<Tuple> getDuplicateResolverForTupleStore(final String tupleStorename) {
		
		if(!cache.containsKey(tupleStorename)) {
			try {
				final TupleStoreAdapter tupleStoreAdapter = ZookeeperClientFactory
						.getZookeeperClient().getTupleStoreAdapter();
				
				final TupleStoreName tupleStoreNameObject = new TupleStoreName(tupleStorename);

				if(! tupleStoreAdapter.isTableKnown(tupleStoreNameObject)) {
					logger.error("Table {} is not known, using do nothing duplicate resolver", tupleStorename);
					return new DoNothingDuplicateResolver();
				}
				
				final TupleStoreConfiguration tupleStoreConfiguration = tupleStoreAdapter.readTuplestoreConfiguration(tupleStoreNameObject);
				final DuplicateResolver<Tuple> resolver = TupleDuplicateResolverFactory.build(tupleStoreConfiguration);
				cache.put(tupleStorename, resolver);
			} catch (ZookeeperException e) {
				logger.error("Exception while reading zookeeper data", e);
				return new DoNothingDuplicateResolver();
			}
		}
		
		final DuplicateResolver<Tuple> duplicateResolver = cache.get(tupleStorename);
			
		return duplicateResolver;
	}
	
	/**
	 * Clear the cache
	 */
	public synchronized void clear() {
		cache.clear();
	}

}
