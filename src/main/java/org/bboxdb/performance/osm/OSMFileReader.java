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
package org.bboxdb.performance.osm;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.bboxdb.performance.osm.filter.OSMTagEntityFilter;
import org.bboxdb.performance.osm.filter.multipoint.OSMBuildingsEntityFilter;
import org.bboxdb.performance.osm.filter.multipoint.OSMRoadsEntityFilter;
import org.bboxdb.performance.osm.filter.multipoint.OSMWaterEntityFilter;
import org.bboxdb.performance.osm.filter.singlepoint.OSMTrafficSignalEntityFilter;
import org.bboxdb.performance.osm.filter.singlepoint.OSMTreeEntityFilter;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import crosby.binary.osmosis.OsmosisReader;

public class OSMFileReader implements Runnable {

	/**
	 * The filter
	 */
	protected final static Map<OSMType, OSMTagEntityFilter> filter = new HashMap<>();
	
	/**
	 * The filename to parse
	 */
	protected final String filename;
	
	/**
	 * The type to import
	 */
	protected final OSMType type;
	
	/**
	 * The callback for completed objects
	 */
	protected final OSMStructureCallback structureCallback;
	
	static {
		filter.put(OSMType.TREE, new OSMTreeEntityFilter());
		filter.put(OSMType.TRAFFIC_SIGNAL, new OSMTrafficSignalEntityFilter());
		filter.put(OSMType.ROAD, new OSMRoadsEntityFilter());
		filter.put(OSMType.BUILDING, new OSMBuildingsEntityFilter());
		filter.put(OSMType.WATER, new OSMWaterEntityFilter());
	}
	
	public OSMFileReader(final String filename, final OSMType type, final OSMStructureCallback structureCallback) {
		super();
		this.filename = filename;
		this.type = type;
		this.structureCallback = structureCallback;
	}
	
	/**
	 * Get the names of the available filter
	 * @return
	 */
	public static String getFilterNames() {
		return getAllFilter()
				.stream()
				.map(o -> o.getName())
				.collect(Collectors.joining("|"));
	}

	/**
	 * Get all known filter
	 * @return
	 */
	public static Set<OSMType> getAllFilter() {
		return Collections.unmodifiableSet(filter.keySet());
	}

	/**
	 * Run the importer
	 * @throws ExecutionException 
	 */
	@Override
	public void run() {
		try {
			final OsmosisReader reader = new OsmosisReader(new FileInputStream(filename));
			
			if(! filter.containsKey(type)) {
				throw new IllegalArgumentException("Unknown filter: " + type);
			}
			
			final OSMTagEntityFilter entityFilter = filter.get(type);
			final Sink sink = getSinkForFilter(entityFilter);
			
			reader.setSink(sink);
			reader.run();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Get the sink for the filter
	 * @param entityFilter
	 * @return 
	 */
	protected Sink getSinkForFilter(OSMTagEntityFilter entityFilter) {
		if(entityFilter.isMultiPointFilter()) {
			return new OSMMultiPointSink(entityFilter, structureCallback);
		} else {
			return new OSMSinglePointSink(entityFilter, structureCallback);
		}
	}
	
}
