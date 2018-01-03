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
package org.bboxdb.storage.entity;

import java.util.concurrent.TimeUnit;

import org.bboxdb.storage.util.UpdateAnomalyResolver;

public class TupleStoreConfigurationBuilder {

	protected final TupleStoreConfiguration ssTableConfiguration;
	
	protected TupleStoreConfigurationBuilder() {
		ssTableConfiguration = new TupleStoreConfiguration();
	}
	
	/**
	 * Create a new configuration builder
	 * @return
	 */
	public static TupleStoreConfigurationBuilder create() {
		return new TupleStoreConfigurationBuilder();
	}
	
	/**
	 * Allow duplicates
	 * @param duplicates
	 * @return
	 */
	public TupleStoreConfigurationBuilder allowDuplicates(final boolean duplicates) {
		ssTableConfiguration.setAllowDuplicates(duplicates);
		return this;
	}
	
	/**
	 * Set the number of tuple ttls
	 * @param ttl
	 * @return
	 */
	public TupleStoreConfigurationBuilder withTTL(final long ttl, final TimeUnit timeUnit) {
		ssTableConfiguration.setTtl(timeUnit.toMillis(ttl));
		return this;
	}
	
	/**
	 * Set the number of versions
	 * @param versions
	 * @return
	 */
	public TupleStoreConfigurationBuilder withVersions(final int versions) {
		ssTableConfiguration.setVersions(versions);
		return this;
	}
	
	/**
	 * Use the spatial index reader
	 * @param spatialIndexReader
	 * @return
	 */
	public TupleStoreConfigurationBuilder withSpatialIndexReader(final String spatialIndexReader) {
		ssTableConfiguration.setSpatialIndexReader(spatialIndexReader);
		return this;
	}

	/**
	 * Use the spatial index writer
	 * @param spatialIndexWriter
	 * @return
	 */
	public TupleStoreConfigurationBuilder withSpatialIndexWriter(final String spatialIndexWriter) {
		ssTableConfiguration.setSpatialIndexWriter(spatialIndexWriter);
		return this;
	}

	/**
	 * The update anomaly resolver
	 * @param updateAnomalyResolver
	 * @return
	 */
	public TupleStoreConfigurationBuilder withUpdateAnomalyResolver(
			final UpdateAnomalyResolver updateAnomalyResolver) {
		
		ssTableConfiguration.setUpdateAnomalyResolver(updateAnomalyResolver);
		return this;
	}
	
	/**
	 * Return the resulting configuration object
	 * @return
	 */
	public TupleStoreConfiguration build() {
		return ssTableConfiguration;
	}

}
