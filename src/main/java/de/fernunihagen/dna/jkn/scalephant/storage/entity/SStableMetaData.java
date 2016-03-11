package de.fernunihagen.dna.jkn.scalephant.storage.entity;

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

	public SStableMetaData() {
		
	}
	
	public SStableMetaData(final long oldestTuple, final long newestTuple, final float[] boundingBoxData) {
		super();
		this.oldestTuple = oldestTuple;
		this.newestTuple = newestTuple;
		this.boundingBoxData = boundingBoxData;
	}
	
	/**
	 * Export the data to YAML
	 * @return
	 */
	public String exportToYaml() {
		
	    final Map<String, Object> data = new HashMap<String, Object>();
	    final Yaml yaml = new Yaml();
	    
		final int dimensions = boundingBoxData.length / 2;
	    
	    data.put("OldestTuple", oldestTuple);
	    data.put("NewestTuple", newestTuple);
		data.put("Dimensions", dimensions);
	    data.put("BoundingBoxData", boundingBoxData);
	    
	    return yaml.dump(data);
	}
	
	/**
	 * Create a instance from yaml data
	 * 
	 * @param yaml
	 * @return
	 */
	public static SStableMetaData importFromYaml(final String yamlString) {
		  final Yaml yaml = new Yaml(); 
	      return yaml.loadAs(yamlString, SStableMetaData.class);
	}

	@Override
	public String toString() {
		return "SStableMetaData [oldestTuple=" + oldestTuple + ", newestTuple="
				+ newestTuple + ", boundingBoxData="
				+ Arrays.toString(boundingBoxData) + "]";
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

}
