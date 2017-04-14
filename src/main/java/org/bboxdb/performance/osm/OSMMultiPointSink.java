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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.bboxdb.performance.osm.filter.OSMTagEntityFilter;
import org.bboxdb.performance.osm.util.Polygon;
import org.bboxdb.performance.osm.util.SerializableNode;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

public class OSMMultiPointSink implements Sink {

	/**
	 * The entity filter
	 */
	protected final OSMTagEntityFilter entityFilter;

	/**
	 * The structure callback
	 */
	protected OSMStructureCallback structureCallback;

	/**
	 * The sqlite connection
	 */
    protected Connection connection;
    
    /**
     * The insert node statement
     */
    protected PreparedStatement insertNode;
    
    /**
     * The select node statement
     */
    protected PreparedStatement selectNode;

	
	protected OSMMultiPointSink(final OSMTagEntityFilter entityFilter, 
			final OSMStructureCallback structureCallback) {
		
		this.entityFilter = entityFilter;
		this.structureCallback = structureCallback;
    	
		try {			
			connection = DriverManager.getConnection("jdbc:h2:file:/tmp/sample.db");
			Statement statement = connection.createStatement();
			
			statement.executeUpdate("drop table if exists osmnode");
			statement.executeUpdate("create table osmnode (id INTEGER, data BLOB)");
			statement.close();
			
			insertNode = connection.prepareStatement("INSERT into osmnode (id, data) values (?,?)");
			selectNode = connection.prepareStatement("SELECT data from osmnode where id = ?");
		
		} catch (SQLException e) {
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
		try {
			final Node node = (Node) entityContainer.getEntity();
			
			final SerializableNode serializableNode = new SerializableNode(node);
			final byte[] nodeBytes = serializableNode.toByteArray();
			final InputStream is = new ByteArrayInputStream(nodeBytes);
			
			insertNode.setLong(1, node.getId());
			insertNode.setBlob(2, is);
			insertNode.execute();
			
			is.close();
		} catch (SQLException | IOException e) {
			throw new RuntimeException(e);
		} 
	}

	/**
	 * Handle a way
	 * @param entityContainer
	 */
	protected void handleWay(final EntityContainer entityContainer) {
		final Way way = (Way) entityContainer.getEntity();

		if(entityFilter.match(way.getTags())) {
			try {
				insertWay(way);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}	
		}
	}
	
	/**
	 * Handle the given way
	 * @param way
	 * @throws SQLException 
	 */
	protected void insertWay(final Way way) throws SQLException {
		final Polygon geometricalStructure = new Polygon(way.getId());
		
		for(final WayNode wayNode : way.getWayNodes()) {
			
			selectNode.setLong(1, wayNode.getNodeId());
			final ResultSet result = selectNode.executeQuery();
			
			if(! result.next() ) {
				System.err.println("Unable to find node for way: " + wayNode.getNodeId());
				return;
			}
			
			final byte[] nodeBytes = result.getBytes(1);
			result.close();
			
			final SerializableNode serializableNode = SerializableNode.fromByteArray(nodeBytes);
			geometricalStructure.addPoint(serializableNode.getLatitude(), serializableNode.getLongitude());
		}
		
		if(geometricalStructure.getNumberOfPoints() > 0) {
				structureCallback.processStructure(geometricalStructure);				
		}
	}

}