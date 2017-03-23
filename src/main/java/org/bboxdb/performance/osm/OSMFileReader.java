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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.bboxdb.performance.osm.filter.multipoint.OSMBuildingsEntityFilter;
import org.bboxdb.performance.osm.filter.multipoint.OSMMultiPointEntityFilter;
import org.bboxdb.performance.osm.filter.multipoint.OSMRoadsEntityFilter;
import org.bboxdb.performance.osm.filter.multipoint.OSMWaterEntityFilter;
import org.bboxdb.performance.osm.filter.singlepoint.OSMSinglePointEntityFilter;
import org.bboxdb.performance.osm.filter.singlepoint.OSMTrafficSignalEntityFilter;
import org.bboxdb.performance.osm.filter.singlepoint.OSMTreeEntityFilter;
import org.bboxdb.performance.osm.util.Polygon;
import org.bboxdb.performance.osm.util.SerializableNode;
import org.bboxdb.performance.osm.util.SerializerHelper;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import crosby.binary.osmosis.OsmosisReader;

public class OSMFileReader implements Runnable {

	/**
	 * The single point filter
	 */
	protected final static Map<OSMType, OSMSinglePointEntityFilter> singlePointFilter = new HashMap<>();
	
	/**
	 * The multi point filter
	 */
	protected final static Map<OSMType, OSMMultiPointEntityFilter> multiPointFilter = new HashMap<>();
	
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
		singlePointFilter.put(OSMType.TRAFFIC_SIGNALS, new OSMTrafficSignalEntityFilter());
		
		multiPointFilter.put(OSMType.ROADS, new OSMRoadsEntityFilter());
		multiPointFilter.put(OSMType.BUILDINGS, new OSMBuildingsEntityFilter());
		multiPointFilter.put(OSMType.WATER, new OSMWaterEntityFilter());
	}
	
	public OSMFileReader(final String filename, final OSMType type, final OSMStructureCallback structureCallback) {
		super();
		this.filename = filename;
		this.type = type;
		this.structureCallback = structureCallback;
	}

	protected class OSMSinglePointSink implements Sink {

		/**
		 * The entity filter
		 */
		private final OSMSinglePointEntityFilter entityFilter;

		protected OSMSinglePointSink(final OSMSinglePointEntityFilter entityFilter) {
			this.entityFilter = entityFilter;
		}

		@Override
		public void release() {
		}

		@Override
		public void complete() {
		}

		@Override
		public void initialize(final Map<String, Object> arg0) {
		}

		@Override
		public void process(final EntityContainer entityContainer) {
			
			if(entityContainer.getEntity() instanceof Node) {
				final Node node = (Node) entityContainer.getEntity();						
				
				if(entityFilter.forwardNode(node)) {
					final Polygon geometricalStructure = new Polygon(node.getId());
					geometricalStructure.addPoint(node.getLatitude(), node.getLongitude());
					structureCallback.processStructure(geometricalStructure);
				}
			}
		}
	}

	protected class OSMMultipointSink implements Sink {
		
		/**
		 * The db instance
		 */
		protected final DB db;
		
		/**
		 * The node map
		 */
		protected final Map<Long, byte[]> nodeMap;
		
		/**
		 * The entity filter
		 */
		protected final OSMMultiPointEntityFilter entityFilter;
		
		/**
		 * The node serializer
		 */
		protected final SerializerHelper<SerializableNode> serializerHelper = new SerializerHelper<>();

		protected OSMMultipointSink(final OSMMultiPointEntityFilter entityFilter) {
			this.entityFilter = entityFilter;
	    	
			try {
				final File dbFile = File.createTempFile("osm-db", ".tmp");
				dbFile.delete();
				
				// Use a disk backed map, to process files > Memory
				this.db = DBMaker.fileDB(dbFile).fileMmapEnableIfSupported().fileDeleteAfterClose().make();
				this.nodeMap = db.hashMap("osm-id-map").keySerializer(Serializer.LONG)
				        .valueSerializer(Serializer.BYTE_ARRAY)
				        .create();
				
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
				
		}

		@Override
		public void initialize(Map<String, Object> arg0) {
			
		}

		@Override
		public void complete() {
			
		}

		@Override
		public void release() {
			
		}

		@Override
		public void process(final EntityContainer entityContainer) {
			try {
				if(entityContainer.getEntity() instanceof Node) {
					final Node node = (Node) entityContainer.getEntity();
					final SerializableNode serializableNode = new SerializableNode(node);
					nodeMap.put(node.getId(), serializerHelper.toByteArray(serializableNode));
				} else if(entityContainer.getEntity() instanceof Way) {
					final Way way = (Way) entityContainer.getEntity();
					final boolean forward = entityFilter.forwardNode(way.getTags());

					if(forward) {
						insertWay(way, nodeMap);	
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		/**
		 * Handle the given way
		 * @param way
		 * @param nodeMap 
		 */
		protected void insertWay(final Way way, final Map<Long, byte[]> nodeMap) {
			final Polygon geometricalStructure = new Polygon(way.getId());
			
			try {
				for(final WayNode wayNode : way.getWayNodes()) {
					
					if(! nodeMap.containsKey(wayNode.getNodeId())) {
						System.err.println("Unable to find node for way: " + wayNode.getNodeId());
						return;
					}
					
					final byte[] nodeBytes = nodeMap.get(wayNode.getNodeId());
					final SerializableNode serializableNode = serializerHelper.loadFromByteArray(nodeBytes);
					geometricalStructure.addPoint(serializableNode.getLatitude(), serializableNode.getLongitude());
				}
				
				if(geometricalStructure.getNumberOfPoints() > 0) {
						structureCallback.processStructure(geometricalStructure);				
				}
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
			
		}
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
				final OSMSinglePointEntityFilter entityFilter = singlePointFilter.get(type);
				reader.setSink(new OSMSinglePointSink(entityFilter));
			}
			
			if(multiPointFilter.containsKey(type)) {
				final OSMMultiPointEntityFilter entityFilter = multiPointFilter.get(type);			
				reader.setSink(new OSMMultipointSink(entityFilter));
			}
			
			reader.run();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
}
