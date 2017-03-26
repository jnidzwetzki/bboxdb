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
import java.util.HashSet;
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

import crosby.binary.osmosis.OsmosisReader;

public class OSMFileReader implements Runnable {

	/**
	 * The single point filter
	 */
	protected final static Map<OSMType, OSMTagEntityFilter> singlePointFilter = new HashMap<>();
	
	/**
	 * The multi point filter
	 */
	protected final static Map<OSMType, OSMTagEntityFilter> multiPointFilter = new HashMap<>();
	
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
		singlePointFilter.put(OSMType.TREE, new OSMTreeEntityFilter());
		singlePointFilter.put(OSMType.TRAFFIC_SIGNAL, new OSMTrafficSignalEntityFilter());
		
		multiPointFilter.put(OSMType.ROAD, new OSMRoadsEntityFilter());
		multiPointFilter.put(OSMType.BUILDING, new OSMBuildingsEntityFilter());
		multiPointFilter.put(OSMType.WATER, new OSMWaterEntityFilter());
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
		final Set<OSMType> names = new HashSet<>();
		names.addAll(singlePointFilter.keySet());
		names.addAll(multiPointFilter.keySet());
		return Collections.unmodifiableSet(names);
	}

	/**
	 * Run the importer
	 * @throws ExecutionException 
	 */
	@Override
	public void run() {
		try {
			final OsmosisReader reader = new OsmosisReader(new FileInputStream(filename));
			
			if(singlePointFilter.containsKey(type)) {
				final OSMTagEntityFilter entityFilter = singlePointFilter.get(type);
				final OSMSinglePointSink sink = new OSMSinglePointSink(entityFilter, structureCallback);
				reader.setSink(sink);
			}
			
			if(multiPointFilter.containsKey(type)) {
				final OSMTagEntityFilter entityFilter = multiPointFilter.get(type);			
				final OSMMultiPointSink sink = new OSMMultiPointSink(entityFilter, structureCallback);
				reader.setSink(sink);
			}
			
			reader.run();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
}
