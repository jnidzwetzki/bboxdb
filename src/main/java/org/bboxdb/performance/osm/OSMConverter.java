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

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.bboxdb.performance.experiments.DetermineSamplingSize;
import org.bboxdb.performance.osm.filter.OSMTagEntityFilter;
import org.bboxdb.performance.osm.filter.multipoint.OSMBuildingsEntityFilter;
import org.bboxdb.performance.osm.filter.multipoint.OSMRoadsEntityFilter;
import org.bboxdb.performance.osm.filter.multipoint.OSMWaterEntityFilter;
import org.bboxdb.performance.osm.filter.singlepoint.OSMTrafficSignalEntityFilter;
import org.bboxdb.performance.osm.filter.singlepoint.OSMTreeEntityFilter;
import org.bboxdb.performance.osm.util.Polygon;
import org.bboxdb.performance.osm.util.SerializableNode;
import org.bboxdb.performance.osm.util.SerializerHelper;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crosby.binary.osmosis.OsmosisReader;

public class OSMConverter implements Runnable, Sink {
	
	/**
	 * The file to import
	 */
	protected final String filename;
	
	/**
	 * The output dir
	 */
	protected final String output;
	
	/**
	 * The node serializer
	 */
	protected final SerializerHelper<Polygon> serializerHelper = new SerializerHelper<>();

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
    
    /**
     * The number of processed elements
     */
    protected long processedElements = 0;
    
    /**
     * The H2 DB file flags
     */
    protected final static String DB_FLAGS = ";LOG=0;CACHE_SIZE=262144;LOCK_MODE=0;UNDO_LOG=0";
    
	/**
	 * The filter
	 */
	protected final static Map<OSMType, OSMTagEntityFilter> filter = new HashMap<>();
	
	/**
	 * The output stream map
	 */
	protected final Map<OSMType, Writer> writerMap = new HashMap<>();
	
	/**
	 * The performance timestamp
	 */
	protected long lastPerformaceTimestamp = 0;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DetermineSamplingSize.class);

	static {
		filter.put(OSMType.TREE, new OSMTreeEntityFilter());
		filter.put(OSMType.TRAFFIC_SIGNAL, new OSMTrafficSignalEntityFilter());
		filter.put(OSMType.ROAD, new OSMRoadsEntityFilter());
		filter.put(OSMType.BUILDING, new OSMBuildingsEntityFilter());
		filter.put(OSMType.WATER, new OSMWaterEntityFilter());
	}
	
	public OSMConverter(final String filename, final String workfolder, final String output) {
		this.filename = filename;
		this.output = output;
		
		try {			
			final File workfoderDir = new File(workfolder);
			workfoderDir.mkdirs();
			
			connection = DriverManager.getConnection("jdbc:h2:nio:" + workfolder + "/osm.db" + DB_FLAGS);
			Statement statement = connection.createStatement();
			
			statement.executeUpdate("DROP TABLE if exists osmnode");
			statement.executeUpdate("CREATE TABLE osmnode (id INTEGER, data BLOB)");
			statement.close();
			
			insertNode = connection.prepareStatement("INSERT into osmnode (id, data) values (?,?)");
			selectNode = connection.prepareStatement("SELECT data from osmnode where id = ?");
		
			connection.commit();
		} catch (SQLException e) {
			throw new IllegalArgumentException(e);
		}
	}
	

	@Override
	public void run() {
		try {
			// Open file handles
			for(final OSMType osmType : filter.keySet()) {
				final BufferedWriter bw = new BufferedWriter(new FileWriter(new File(output + File.separator + osmType.toString())));
				writerMap.put(osmType, bw);
			}
			
			System.out.format("Importing %s\n", filename);
			final OsmosisReader reader = new OsmosisReader(new FileInputStream(filename));
			reader.setSink(this);
			reader.run();
			System.out.format("Imported %d objects\n", processedElements);
			
			// Close file handles
			for(final Writer writer : writerMap.values()) {
				writer.close();
			}
			
			writerMap.clear();
			
		} catch (IOException e) {
			logger.error("Got an exception during import", e);
		}
	}

	@Override
	public void initialize(Map<String, Object> metaData) {		
	}

	@Override
	public void complete() {		
	}

	@Override
	public void release() {		
	}
	
	@Override
	public void process(final EntityContainer entityContainer) {
		
		if(processedElements % 10000 == 0) {
			double performance = 0;
			if(lastPerformaceTimestamp != 0) {
				performance = 10000.0 / ((System.currentTimeMillis() - lastPerformaceTimestamp) / 1000.0);
			}
			logger.info("Processing element {} / Elements per Sec", processedElements, performance);
			lastPerformaceTimestamp = System.currentTimeMillis();
		}
		
		if(entityContainer.getEntity() instanceof Node) {
			handleNode(entityContainer);
		} else if(entityContainer.getEntity() instanceof Way) {
			handleWay(entityContainer);
		}
		
		processedElements++;
	}

	/**
	 * Handle a node
	 * @param entityContainer
	 */
	protected void handleNode(final EntityContainer entityContainer) {
		try {
			
			final Node node = (Node) entityContainer.getEntity();
			
			for(final OSMType osmType : filter.keySet()) {
				final OSMTagEntityFilter entityFilter = filter.get(osmType);
				if(entityFilter.match(node.getTags())) {
					final Polygon geometricalStructure = new Polygon(node.getId());
					geometricalStructure.addPoint(node.getLatitude(), node.getLongitude());
					
					for(final Tag tag : node.getTags()) {
						geometricalStructure.addProperty(tag.getKey(), tag.getValue());
					}
					
					final Writer writer = writerMap.get(osmType);
					writer.write(geometricalStructure.toGeoJson());
					writer.write("\n");
				}
			}
			
			final SerializableNode serializableNode = new SerializableNode(node);
			final byte[] nodeBytes = serializableNode.toByteArray();
			final InputStream is = new ByteArrayInputStream(nodeBytes);
			
			insertNode.setLong(1, node.getId());
			insertNode.setBlob(2, is);
			insertNode.execute();
			is.close();

			connection.commit();
			
		} catch (SQLException | IOException e) {
			throw new RuntimeException(e);
		} 
	}

	/**
	 * Handle a way
	 * @param entityContainer
	 */
	protected void handleWay(final EntityContainer entityContainer) {
		try {
			final Way way = (Way) entityContainer.getEntity();
			
			for(final OSMType osmType : filter.keySet()) {
				
				final OSMTagEntityFilter entityFilter = filter.get(osmType);
				if(entityFilter.match(way.getTags())) {
					
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
					
					for(final Tag tag : way.getTags()) {
						geometricalStructure.addProperty(tag.getKey(), tag.getValue());
					}
					
					final Writer writer = writerMap.get(osmType);
					writer.write(geometricalStructure.toGeoJson());
					writer.write("\n");
				}
			}
		} catch (SQLException | IOException e) {
			throw new RuntimeException(e);
		} 
	}

	
	/**
	 * ====================================================
	 * Main * Main * Main * Main * Main
	 * ====================================================
	 */
	public static void main(final String[] args) {
		// Check parameter
		if(args.length != 3) {
			System.err.println("Usage: programm <filename> <work folder> <output dir>");
			System.exit(-1);
		}
		
		final String filename = args[0];
		final String workfolder = args[1];
		final String output = args[2];
		
		// Check file
		final File inputFile = new File(filename);
		if(! inputFile.isFile()) {
			System.err.println("Unable to open file: " + filename);
			System.exit(-1);
		}
		
		// Check output dir
		final File outputDir = new File(output);
		if(outputDir.exists() ) {
			System.err.println("Output dir already exist, please remove first");
			System.exit(-1);
		}
		
		if(! outputDir.mkdirs() ) {
			System.err.println("Unable to create " + output);
			System.exit(-1);
		}

		// Check database file
		final File file = new File(workfolder);
		if(file.exists()) {
			System.err.println("Work folder already exists, exiting...");
			System.exit(-1);
		}

		final OSMConverter determineSamplingSize = new OSMConverter(filename, workfolder, output);
		determineSamplingSize.run();
	}

}
