package de.fernunihagen.dna.jkn.scalephant.storage.entity;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class SStableMetaData {
	
	/**
	 * The timestamp of the oldest tuple
	 */
	protected long oldestTuple = Long.MAX_VALUE;
	
	/**
	 * The timestamp of the newest tuple
	 */
	protected long newestTuple = Long.MIN_VALUE;
	
	/**
	 * The bounding box of all tuples
	 */
	protected float[] boundingBoxData;
	
	/**
	 * The dimensions of the bounding box
	 */
	protected int dimensions;

	public SStableMetaData() {
		
	}
	
	public SStableMetaData(final long oldestTuple, final long newestTuple, final float[] boundingBoxData) {
		super();
		this.oldestTuple = oldestTuple;
		this.newestTuple = newestTuple;
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
	public void exportToYamlFile(final String filename) throws IOException {
	    final Map<String, Object> data = getPropertyMap();
	    
	    final FileWriter writer = new FileWriter(filename);
	    
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
	    data.put("oldestTuple", oldestTuple);
	    data.put("newestTuple", newestTuple);
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
	public static SStableMetaData importFromYaml(final String yamlString) {
		  final Yaml yaml = new Yaml(); 
	      return yaml.loadAs(yamlString, SStableMetaData.class);
	}
	
	/**
	 * Create a instance from yaml data - read data from file
	 * 
	 * @param filename
	 * @return
	 * @throws FileNotFoundException
	 */
	public static SStableMetaData importFromYamlFile(final String filename) throws FileNotFoundException {
		  final Yaml yaml = new Yaml(); 
		  final FileReader reader = new FileReader(filename);
	      return yaml.loadAs(reader, SStableMetaData.class);
	}

	public long getOldestTuple() {
		return oldestTuple;
	}

	public void setOldestTuple(final long oldestTuple) {
		this.oldestTuple = oldestTuple;
	}

	public long getNewestTuple() {
		return newestTuple;
	}

	public void setNewestTuple(final long newestTuple) {
		this.newestTuple = newestTuple;
	}

	public float[] getBoundingBoxData() {
		return boundingBoxData;
	}

	public void setBoundingBoxData(final float[] boundingBoxData) {
		this.boundingBoxData = boundingBoxData;
	}

	public int getDimensions() {
		return dimensions;
	}

	public void setDimensions(int dimensions) {
		this.dimensions = dimensions;
	}

	@Override
	public String toString() {
		return "SStableMetaData [oldestTuple=" + oldestTuple + ", newestTuple="
				+ newestTuple + ", boundingBoxData="
				+ Arrays.toString(boundingBoxData) + ", dimensions="
				+ dimensions + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(boundingBoxData);
		result = prime * result + dimensions;
		result = prime * result + (int) (newestTuple ^ (newestTuple >>> 32));
		result = prime * result + (int) (oldestTuple ^ (oldestTuple >>> 32));
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
		SStableMetaData other = (SStableMetaData) obj;
		if (!Arrays.equals(boundingBoxData, other.boundingBoxData))
			return false;
		if (dimensions != other.dimensions)
			return false;
		if (newestTuple != other.newestTuple)
			return false;
		if (oldestTuple != other.oldestTuple)
			return false;
		return true;
	}

}
