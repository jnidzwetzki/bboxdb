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
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.bboxdb.performance.osm.filter.OSMTagEntityFilter;
import org.bboxdb.performance.osm.util.Polygon;
import org.bboxdb.performance.osm.util.SerializableNode;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

public class OSMMultiPointSink implements Sink {
	
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
	protected final OSMTagEntityFilter entityFilter;

	/**
	 * The structure callback
	 */
	protected OSMStructureCallback structureCallback;
	
	/**
	 * The operation mode
	 */
	protected OSMMultiPointMode operationMode = OSMMultiPointMode.PREPROCESSING;
	
	/**
	 * A set of all required nodes
	 */
	protected Set<Long> requiredNodes = new HashSet<>();
	
	protected OSMMultiPointSink(final OSMTagEntityFilter entityFilter, 
			final OSMStructureCallback structureCallback) {
		
		this.entityFilter = entityFilter;
		this.structureCallback = structureCallback;
    	
		try {
			final File dbFile = File.createTempFile("osm-db", ".tmp");
			dbFile.delete();
			
			// Use a disk backed map, to process files > Memory
			this.db = DBMaker
					.fileDB(dbFile)
				    .allocateStartSize(1 * 1024 * 1024 * 1024)  // 1 GB
				    .allocateIncrement(512 * 1024 * 1024)       // 512 MB
					.fileMmapEnableIfSupported()
					.fileDeleteAfterClose()
					.make();
			
			this.nodeMap = db
					.hashMap("osm-id-map")
					.keySerializer(Serializer.LONG)
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
		if(entityContainer.getEntity() instanceof Node) {
			handleNode(entityContainer);
		} else if(entityContainer.getEntity() instanceof Way) {
			handleWay(entityContainer);
		}
	}

	/**
	 * Handle a node
	 * @param entityContainer
	 */
	protected void handleNode(final EntityContainer entityContainer) {
		final Node node = (Node) entityContainer.getEntity();
		
		if(operationMode == OSMMultiPointMode.PROCESSING) {
			if(requiredNodes.contains(node.getId())) {
				final SerializableNode serializableNode = new SerializableNode(node);
				nodeMap.put(node.getId(), serializableNode.toByteArray());
			}
		}
	}

	/**
	 * Handle a way
	 * @param entityContainer
	 */
	protected void handleWay(final EntityContainer entityContainer) {
		final Way way = (Way) entityContainer.getEntity();

		if(entityFilter.match(way.getTags())) {
			
			// Store only required nodes
			if(operationMode == OSMMultiPointMode.PREPROCESSING) {
				for(final WayNode wayNode : way.getWayNodes()) {
					requiredNodes.add(wayNode.getNodeId());
				}
			} else {
				// Handle way
				insertWay(way, nodeMap);	
			}
		}
	}
	
	/**
	 * Handle the given way
	 * @param way
	 * @param nodeMap 
	 */
	protected void insertWay(final Way way, final Map<Long, byte[]> nodeMap) {
		final Polygon geometricalStructure = new Polygon(way.getId());
		
		for(final WayNode wayNode : way.getWayNodes()) {
			
			if(! nodeMap.containsKey(wayNode.getNodeId())) {
				System.err.println("Unable to find node for way: " + wayNode.getNodeId());
				return;
			}
			
			final byte[] nodeBytes = nodeMap.get(wayNode.getNodeId());
			final SerializableNode serializableNode = SerializableNode.fromByteArray(nodeBytes);
			geometricalStructure.addPoint(serializableNode.getLatitude(), serializableNode.getLongitude());
		}
		
		if(geometricalStructure.getNumberOfPoints() > 0) {
				structureCallback.processStructure(geometricalStructure);				
		}
	}
	
	/**
	 * Set the operation mode
	 * @param operationMode
	 */
	public void setOperationMode(final OSMMultiPointMode operationMode) {
		this.operationMode = operationMode;
	}
}