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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class TupleStoreMetaData {
	
	/**
	 * The amount of tuples
	 */
	protected long tuples = 0;
	
	/**
	 * The version timestamp of the oldest tuple
	 */
	protected long oldestTupleVersionTimestamp = Long.MAX_VALUE;
	
	/**
	 * The version timestamp of the newest tuple
	 */
	protected long newestTupleVersionTimestamp = Long.MIN_VALUE;
	
	/**
	 * The inserted timestamp of the newest tuple
	 */
	protected long newestTupleInsertedTimstamp = Long.MIN_VALUE;
	
	/**
	 * The bounding box of all tuples
	 */
	protected double[] boundingBoxData;
	
	/**
	 * The dimensions of the bounding box
	 */
	protected int dimensions;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleStoreMetaData.class);

	/**
	 * Needed for YAML deserializer
	 */
	public TupleStoreMetaData() {
		
	}
	
	public TupleStoreMetaData(final long tuples, final long oldestTuple, final long newestTuple, 
			final long newestTupleInsertedTimstamp, final double[] boundingBoxData) {
		
		this.tuples = tuples;
		this.oldestTupleVersionTimestamp = oldestTuple;
		this.newestTupleVersionTimestamp = newestTuple;
		this.newestTupleInsertedTimstamp = newestTupleInsertedTimstamp;
		this.boundingBoxData = boundingBoxData;
		this.dimensions = boundingBoxData.length / 2;
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
		data.put("tuples", tuples);
	    data.put("oldestTupleVersionTimestamp", oldestTupleVersionTimestamp);
	    data.put("newestTupleVersionTimestamp", newestTupleVersionTimestamp);
	    data.put("newestTupleInsertedTimstamp", newestTupleInsertedTimstamp);
		data.put("dimensions", dimensions);
	    data.put("boundingBoxData", boundingBoxData);
		return data;
	}
	
	/**
	 * Create a instance from yaml data - read data from string
	 * 
	 * @param yaml
	 * @return
	 */
	public static TupleStoreMetaData importFromYaml(final String yamlString) {
		  final Yaml yaml = new Yaml(); 
	      return yaml.loadAs(yamlString, TupleStoreMetaData.class);
	}
	
	/**
	 * Create a instance from yaml data - read data from file
	 * 
	 * @param tmpFile
	 * @return
	 * @throws FileNotFoundException
	 */
	public static TupleStoreMetaData importFromYamlFile(final File tmpFile) {
		  final Yaml yaml = new Yaml(); 
		  FileReader reader;
		try {
			reader = new FileReader(tmpFile);
		} catch (FileNotFoundException e) {
			logger.warn("Unable to load file: " + tmpFile, e);
			return null;
		}
		
		return yaml.loadAs(reader, TupleStoreMetaData.class);
	}

	public long getOldestTupleVersionTimestamp() {
		return oldestTupleVersionTimestamp;
	}

	public void setOldestTupleVersionTimestamp(final long oldestTuple) {
		this.oldestTupleVersionTimestamp = oldestTuple;
	}

	public long getNewestTupleVersionTimestamp() {
		return newestTupleVersionTimestamp;
	}

	public void setNewestTupleVersionTimestamp(final long newestTuple) {
		this.newestTupleVersionTimestamp = newestTuple;
	}

	
	public long getNewestTupleInsertedTimstamp() {
		return newestTupleInsertedTimstamp;
	}

	public void setNewestTupleInsertedTimstamp(final long newestTupleInsertedTimstamp) {
		this.newestTupleInsertedTimstamp = newestTupleInsertedTimstamp;
	}

	public double[] getBoundingBoxData() {
		return boundingBoxData;
	}

	public void setBoundingBoxData(final double[] boundingBoxData) {
		this.boundingBoxData = boundingBoxData;
	}

	public int getDimensions() {
		return dimensions;
	}

	public void setDimensions(int dimensions) {
		this.dimensions = dimensions;
	}

	public long getTuples() {
		return tuples;
	}

	public void setTuples(long tuples) {
		this.tuples = tuples;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(boundingBoxData);
		result = prime * result + dimensions;
		result = prime * result + (int) (newestTupleInsertedTimstamp ^ (newestTupleInsertedTimstamp >>> 32));
		result = prime * result + (int) (newestTupleVersionTimestamp ^ (newestTupleVersionTimestamp >>> 32));
		result = prime * result + (int) (oldestTupleVersionTimestamp ^ (oldestTupleVersionTimestamp >>> 32));
		result = prime * result + (int) (tuples ^ (tuples >>> 32));
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
		TupleStoreMetaData other = (TupleStoreMetaData) obj;
		if (!Arrays.equals(boundingBoxData, other.boundingBoxData))
			return false;
		if (dimensions != other.dimensions)
			return false;
		if (newestTupleInsertedTimstamp != other.newestTupleInsertedTimstamp)
			return false;
		if (newestTupleVersionTimestamp != other.newestTupleVersionTimestamp)
			return false;
		if (oldestTupleVersionTimestamp != other.oldestTupleVersionTimestamp)
			return false;
		if (tuples != other.tuples)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SStableMetaData [tuples=" + tuples + ", oldestTupleVersionTimestamp=" + oldestTupleVersionTimestamp
				+ ", newestTupleVersionTimestamp=" + newestTupleVersionTimestamp + ", newestTupleInsertedTimstamp="
				+ newestTupleInsertedTimstamp + ", boundingBoxData=" + Arrays.toString(boundingBoxData)
				+ ", dimensions=" + dimensions + "]";
	}

}
