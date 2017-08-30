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
package org.bboxdb.storage.entity;

public class SSTableConfigurationBuilder {

	protected final SSTableConfiguration ssTableConfiguration;
	
	protected SSTableConfigurationBuilder() {
		ssTableConfiguration = new SSTableConfiguration();
	}
	
	/**
	 * Create a new configuration builder
	 * @return
	 */
	public static SSTableConfigurationBuilder create() {
		return new SSTableConfigurationBuilder();
	}
	
	/**
	 * Allow duplicates
	 * @param duplicates
	 * @return
	 */
	public SSTableConfigurationBuilder allowDuplicates(final boolean duplicates) {
		ssTableConfiguration.setAllowDuplicates(duplicates);
		return this;
	}
	
	/**
	 * Set the number of tuple ttls
	 * @param ttl
	 * @return
	 */
	public SSTableConfigurationBuilder withTTL(final long ttl) {
		ssTableConfiguration.setTtl(ttl);
		return this;
	}
	
	/**
	 * Set the number of versions
	 * @param versions
	 * @return
	 */
	public SSTableConfigurationBuilder withVersions(final int versions) {
		ssTableConfiguration.setVersions(versions);
		return this;
	}
	
	/**
	 * Use the spatial index reader
	 * @param spatialIndexReader
	 * @return
	 */
	public SSTableConfigurationBuilder withSpatialIndexReader(final String spatialIndexReader) {
		ssTableConfiguration.setSpatialIndexReader(spatialIndexReader);
		return this;
	}

	/**
	 * Use the spatial index writer
	 * @param spatialIndexWriter
	 * @return
	 */
	public SSTableConfigurationBuilder withSpatialIndexWriter(final String spatialIndexWriter) {
		ssTableConfiguration.setSpatialIndexWriter(spatialIndexWriter);
		return this;
	}

	/**
	 * Return the resulting configuration object
	 * @return
	 */
	public SSTableConfiguration build() {
		return ssTableConfiguration;
	}

}
