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
package org.bboxdb.tools.converter.osm.store;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.tools.converter.osm.util.SerializableNode;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;

public class OSMJDBCNodeStore implements OSMNodeStore {
	
    /**
     * The H2 DB file flags
     */
    protected final static String DB_FLAGS = ";LOG=0;CACHE_SIZE=262144;LOCK_MODE=0;UNDO_LOG=0";
    
	/**
	 * The database connection
	 */
    protected final List<Connection> connections = new ArrayList<>();
    
    /**
     * The insert node statement
     */
    protected final List<PreparedStatement> insertNodeStatements = new ArrayList<>();
    
    /**
     * The select node statement
     */
    protected final List<PreparedStatement> selectNodeStatements = new ArrayList<>();

    /**
     * The number of instances
     */
    protected int instances;

	public OSMJDBCNodeStore(final List<String> baseDir, final long inputLength) {
		
		// 5 instances per GB
		instances = (int) ((inputLength / (Math.pow(1024, 3)) * 5));
		System.out.println("Using DB instances: " + instances);
				
		try {			
			// Prepare DB_Instances
			for(int i = 0; i < instances; i++) {
				
				final String workfolder = baseDir.get(i % baseDir.size());
				
				final Connection connection = DriverManager.getConnection("jdbc:h2:nio:" + workfolder + "/osm_" + i + ".db" + DB_FLAGS);
				Statement statement = connection.createStatement();
				
				statement.executeUpdate("DROP TABLE if exists osmnode");
				statement.executeUpdate("CREATE TABLE osmnode (id BIGINT PRIMARY KEY, data BLOB)");
				statement.close();
				
				final PreparedStatement insertNode = connection.prepareStatement("INSERT into osmnode (id, data) values (?,?)");
				final PreparedStatement selectNode = connection.prepareStatement("SELECT data from osmnode where id = ?");
			
				insertNodeStatements.add(insertNode);
				selectNodeStatements.add(selectNode);
				
				connection.commit();
				connections.add(connection);
			}
		} catch (SQLException e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	/**
	 * Close all resources
	 */
	public void close() {
		
		selectNodeStatements.stream().forEach(p -> {
			try {
				p.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		});
		selectNodeStatements.clear();
		
		insertNodeStatements.stream().forEach(p -> {
			try {
				p.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		});
		insertNodeStatements.clear();
		
		connections.stream().forEach(p -> {
			try {
				p.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		});
		connections.clear();
	}

	/**
	 * Store a new node
	 * @param node
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public void storeNode(final Node node) throws SQLException, IOException {
		
		final int connectionNumber = getDatabaseForNode(node.getId());
		final Connection connection = connections.get(connectionNumber);
		final PreparedStatement insertNode = insertNodeStatements.get(connectionNumber);
		
		final SerializableNode serializableNode = new SerializableNode(node);
		final byte[] nodeBytes = serializableNode.toByteArray();
		final InputStream is = new ByteArrayInputStream(nodeBytes);
		
		insertNode.setLong(1, node.getId());
		insertNode.setBlob(2, is);
		insertNode.execute();
		is.close();

		connection.commit();
	}

	/**
	 * Get the database for nodes
	 * @param node
	 * @return
	 */
	protected int getDatabaseForNode(final long nodeid) {
		return (int) (nodeid % instances);
	}
	
	/**
	 * Get the id for the node
	 * @param nodeId
	 * @return
	 * @throws SQLException 
	 */
	public SerializableNode getNodeForId(final long nodeId) throws SQLException {
		final int connectionNumber = getDatabaseForNode(nodeId);
		final PreparedStatement selectNode = selectNodeStatements.get(connectionNumber);

		selectNode.setLong(1, nodeId);
		final ResultSet result = selectNode.executeQuery();
		
		if(! result.next()) {
			throw new RuntimeException("Unable to find node for way: " + nodeId);
		}
		
		final byte[] nodeBytes = result.getBytes(1);
		result.close();
		
		final SerializableNode node = SerializableNode.fromByteArray(nodeBytes);
		return node;
	}

	@Override
	public int getInstances() {
		return instances;
	}
}
