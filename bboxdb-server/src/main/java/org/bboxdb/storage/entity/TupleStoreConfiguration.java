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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.util.UpdateAnomalyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class TupleStoreConfiguration {
	
	/**
	 * Allow duplicates
	 */
	protected boolean allowDuplicates = false;
	
	/**
	 * The ttl
	 */
	protected long ttl = 0;
	
	/**
	 * The amount of versions per tuple
	 */
	protected int versions = 0;
	
	/**
	 * The spatial index writer
	 */
	protected String spatialIndexWriter = "org.bboxdb.storage.sstable.spatialindex.rtree.RTreeBuilder";
	
	/**
	 * The spatial index reader
	 */
	protected String spatialIndexReader = "org.bboxdb.storage.sstable.spatialindex.rtree.mmf.RTreeMMFReader";
	
	/**
	 * The update anomaly resolver
	 */
	protected UpdateAnomalyResolver updateAnomalyResolver = UpdateAnomalyResolver.NONE;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleStoreManager.class);

	/**
	 * Needed for YAML deserializer
	 */
	public TupleStoreConfiguration() {

	}
	
	public boolean isAllowDuplicates() {
		return allowDuplicates;
	}

	public void setAllowDuplicates(final boolean allowDuplicates) {
		this.allowDuplicates = allowDuplicates;
	}

	public long getTTL() {
		return ttl;
	}

	public void setTtl(final long ttl) {
		this.ttl = ttl;
	}

	public int getVersions() {
		return versions;
	}

	public void setVersions(final int versions) {
		this.versions = versions;
	}

	public String getSpatialIndexWriter() {
		return spatialIndexWriter;
	}

	public void setSpatialIndexWriter(final String spatialIndexWriter) {
		this.spatialIndexWriter = spatialIndexWriter;
	}

	public String getSpatialIndexReader() {
		return spatialIndexReader;
	}

	public void setSpatialIndexReader(final String spatialIndexReader) {
		this.spatialIndexReader = spatialIndexReader;
	}
	
	public UpdateAnomalyResolver getUpdateAnomalyResolver() {
		return updateAnomalyResolver;
	}
	
	public void setUpdateAnomalyResolver(final UpdateAnomalyResolver updateAnomalyResolver) {
		this.updateAnomalyResolver = updateAnomalyResolver;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (allowDuplicates ? 1231 : 1237);
		result = prime * result + ((spatialIndexReader == null) ? 0 : spatialIndexReader.hashCode());
		result = prime * result + ((spatialIndexWriter == null) ? 0 : spatialIndexWriter.hashCode());
		result = prime * result + (int) (ttl ^ (ttl >>> 32));
		result = prime * result + ((updateAnomalyResolver == null) ? 0 : updateAnomalyResolver.hashCode());
		result = prime * result + versions;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TupleStoreConfiguration other = (TupleStoreConfiguration) obj;
		if (allowDuplicates != other.allowDuplicates)
			return false;
		if (spatialIndexReader == null) {
			if (other.spatialIndexReader != null)
				return false;
		} else if (!spatialIndexReader.equals(other.spatialIndexReader))
			return false;
		if (spatialIndexWriter == null) {
			if (other.spatialIndexWriter != null)
				return false;
		} else if (!spatialIndexWriter.equals(other.spatialIndexWriter))
			return false;
		if (ttl != other.ttl)
			return false;
		if (updateAnomalyResolver == null) {
			if (other.updateAnomalyResolver != null)
				return false;
		} else if (!updateAnomalyResolver.equals(other.updateAnomalyResolver))
			return false;
		if (versions != other.versions)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TupleStoreConfiguration [allowDuplicates=" + allowDuplicates + ", ttl=" + ttl + ", versions=" + versions
				+ ", spatialIndexWriter=" + spatialIndexWriter + ", spatialIndexReader=" + spatialIndexReader
				+ ", updateAnomalyResolver=" + updateAnomalyResolver + "]";
	}

	/**
	 * Export the data to YAML
	 * @return
	 */
	public String exportToYaml() {
	    final Map<String, Object> data = getPropertyMap();
	    
	    final Yaml yaml = new Yaml();
	    return yaml.dump(data);
	}
	
	/**
	 * Export the data to YAML File
	 * @return
	 * @throws IOException 
	 */
	public void exportToYamlFile(final File outputFile) throws IOException {
	    final Map<String, Object> data = getPropertyMap();
	    
	    final FileWriter writer = new FileWriter(outputFile);
	    logger.debug("Output data to: " + outputFile);
	    
	    final Yaml yaml = new Yaml();
	    yaml.dump(data, writer);
	    writer.close();
	}

	/**
	 * Generate a map with the properties of this class
	 * @return
	 */
	protected Map<String, Object> getPropertyMap() {
		final Map<String, Object> data = new HashMap<String, Object>();	
		data.put("allowDuplicates", allowDuplicates);
	    data.put("spatialIndexReader", spatialIndexReader);
	    data.put("spatialIndexWriter", spatialIndexWriter);
	    data.put("ttl", ttl);
		data.put("versions", versions);
		data.put("updateAnomalyResolver", updateAnomalyResolver);
		return data;
	}
	
	/**
	 * Create a instance from yaml data - read data from string
	 * 
	 * @param yaml
	 * @return
	 */
	public static TupleStoreConfiguration importFromYaml(final String yamlString) {
		  final Yaml yaml = new Yaml(); 
	      return yaml.loadAs(yamlString, TupleStoreConfiguration.class);
	}
	
	/**
	 * Create a instance from yaml data - read data from file
	 * 
	 * @param tmpFile
	 * @return
	 * @throws FileNotFoundException
	 */
	public static TupleStoreConfiguration importFromYamlFile(final File tmpFile) {
		  final Yaml yaml = new Yaml(); 
		  FileReader reader;
		try {
			reader = new FileReader(tmpFile);
		} catch (FileNotFoundException e) {
			logger.warn("Unable to load file: " + tmpFile, e);
			return null;
		}
		
		return yaml.loadAs(reader, TupleStoreConfiguration.class);
	}
}
