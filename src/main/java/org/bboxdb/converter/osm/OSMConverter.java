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
package org.bboxdb.converter.osm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.bboxdb.converter.osm.filter.OSMTagEntityFilter;
import org.bboxdb.converter.osm.filter.multipoint.OSMBuildingsEntityFilter;
import org.bboxdb.converter.osm.filter.multipoint.OSMRoadsEntityFilter;
import org.bboxdb.converter.osm.filter.multipoint.OSMWaterEntityFilter;
import org.bboxdb.converter.osm.filter.multipoint.WoodEntityFilter;
import org.bboxdb.converter.osm.filter.singlepoint.OSMTrafficSignalEntityFilter;
import org.bboxdb.converter.osm.filter.singlepoint.OSMTreeEntityFilter;
import org.bboxdb.converter.osm.store.OSMBDBNodeStore;
import org.bboxdb.converter.osm.store.OSMNodeStore;
import org.bboxdb.converter.osm.util.Polygon;
import org.bboxdb.converter.osm.util.SerializableNode;
import org.bboxdb.converter.osm.util.SerializerHelper;
import org.bboxdb.util.ExceptionSafeThread;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crosby.binary.osmosis.OsmosisReader;

public class OSMConverter {
	
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
     * The number element statistics
     */
    protected final OSMConverterStatistics statistics;

	/**
	 * The filter
	 */
	protected final static Map<OSMType, OSMTagEntityFilter> filter = new HashMap<>();
	
	/**
	 * The output stream map
	 */
	protected final Map<OSMType, Writer> writerMap = new HashMap<>();
	
	/**
	 * The node store
	 */
	protected final OSMNodeStore osmNodeStore;

	/**
	 * The thread pool
	 */
	protected final ExecutorService threadPool;
	
	protected final int CONSUMER_THREADS = 20;
	
	/**
	 * The Blocking queue
	 */
	protected BlockingQueue<Way> queue = new ArrayBlockingQueue<>(200);
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(OSMConverter.class);

	static {
		filter.put(OSMType.TREE, new OSMTreeEntityFilter());
		filter.put(OSMType.WOOD, new WoodEntityFilter());
		filter.put(OSMType.TRAFFIC_SIGNAL, new OSMTrafficSignalEntityFilter());
		filter.put(OSMType.ROAD, new OSMRoadsEntityFilter());
		filter.put(OSMType.BUILDING, new OSMBuildingsEntityFilter());
		filter.put(OSMType.WATER, new OSMWaterEntityFilter());
	}
	
	public OSMConverter(final String filename, final String workfolder, final String output) {
		this.filename = filename;
		this.output = output;
		this.statistics = new OSMConverterStatistics();
		
		final File inputFile = new File(filename);

		final List<String> workfolders = Arrays.asList(workfolder.split(":"));
		this.osmNodeStore = new OSMBDBNodeStore(workfolders, inputFile.length());
		
		threadPool = Executors.newCachedThreadPool();	
		
		statistics.start();
	}

	/**
	 * Start the converter
	 */
	public void start() {
		try {
			// Open file handles
			for(final OSMType osmType : filter.keySet()) {
				final BufferedWriter bw = new BufferedWriter(new FileWriter(new File(output + File.separator + osmType.toString())));
				writerMap.put(osmType, bw);
			}
			
			System.out.format("Importing %s\n", filename);
			final OsmosisReader reader = new OsmosisReader(new FileInputStream(filename));
			reader.setSink(new Sink() {
				
				@Override
				public void release() {}
				
				@Override
				public void complete() {}
				
				@Override
				public void initialize(final Map<String, Object> metaData) {}
				
				@Override
				public void process(final EntityContainer entityContainer) {
					try {

						if(entityContainer.getEntity() instanceof Node) {
							// Nodes are cheap to handle, dispatching to another thread 
							// is more expensive
							
							final Node node = (Node) entityContainer.getEntity();
							handleNode(node);
							statistics.incProcessedNodes();
						} else if(entityContainer.getEntity() instanceof Way) {
							// Ways are expensive to handle
							
							final Way way = (Way) entityContainer.getEntity();
							queue.put(way);
							statistics.incProcessedWays();
						}
					} catch (InterruptedException e) {
						return;
					}
				}
			});
			
			// The way consumer
			for(int i = 0; i < CONSUMER_THREADS; i++) {
				threadPool.submit(new Consumer());
			}
			
			reader.run();
		} catch (IOException e) {
			logger.error("Got an exception during import", e);
		} finally {
			shutdown();
		}
	}

	/**
	 * Shutdown the importer
	 */
	protected void shutdown() {
		
		threadPool.shutdownNow();
		
		try {
			threadPool.awaitTermination(120, TimeUnit.SECONDS);
		} catch (InterruptedException e1) {
			Thread.currentThread().interrupt();
			return;
		}
		
		statistics.stop();
		
		// Close file handles
		for(final Writer writer : writerMap.values()) {
			try {
				writer.close();
			} catch (IOException e) {
				logger.error("IO Exception while closing writer");
			}
		}
		
		writerMap.clear();
		osmNodeStore.close();
	}
	
	
	class Consumer extends ExceptionSafeThread {
		/**
		 * The consumer thread
		 */
		@Override
		protected void runThread() {
			
			while(! Thread.currentThread().isInterrupted() ) {
				try {
					final Way way = queue.take();
					handleWay(way);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}		
			}
			
			// Thread is getting ready to die, but first,
	        // handle the pending jobs
			Way way = null;
			while((way = queue.poll()) != null) {
				handleWay(way);
			}
			
		}
	}
	

	/**
	 * Handle a node
	 * @param entityContainer
	 */
	protected void handleNode(final Node node) {
		try {			
			for(final OSMType osmType : filter.keySet()) {
				final OSMTagEntityFilter entityFilter = filter.get(osmType);
				if(entityFilter.match(node.getTags())) {
					final Polygon geometricalStructure = new Polygon(node.getId());
					geometricalStructure.addPoint(node.getLatitude(), node.getLongitude());
					
					for(final Tag tag : node.getTags()) {
						geometricalStructure.addProperty(tag.getKey(), tag.getValue());
					}
					
					writePolygonToOutput(osmType, geometricalStructure);
				}
			}
			
			osmNodeStore.storeNode(node);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
	}

	/**
	 * Write the polygon to output
	 * @param osmType
	 * @param geometricalStructure
	 * @throws IOException
	 */
	protected void writePolygonToOutput(final OSMType osmType, final Polygon geometricalStructure) throws IOException {
		final Writer writer = writerMap.get(osmType);
		synchronized (writer) {
			writer.write(geometricalStructure.toGeoJson());
			writer.write("\n");
		}
	}

	/**
	 * Handle a way
	 * @param entityContainer
	 */
	protected void handleWay(final Way way) {
		try {			
			for(final OSMType osmType : filter.keySet()) {
				
				final OSMTagEntityFilter entityFilter = filter.get(osmType);
				if(entityFilter.match(way.getTags())) {
					
					final Polygon geometricalStructure = new Polygon(way.getId());

					for(final Tag tag : way.getTags()) {
						geometricalStructure.addProperty(tag.getKey(), tag.getValue());
					}
					
					// Perform search async
 					for(final WayNode wayNode : way.getWayNodes()) {
 						final SerializableNode node = osmNodeStore.getNodeForId(wayNode.getNodeId());
 						geometricalStructure.addPoint(node.getLatitude(), node.getLongitude()); 						
					}
 			
					writePolygonToOutput(osmType, geometricalStructure);
				}
			}
		} catch (Exception e) {
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

		final OSMConverter converter = new OSMConverter(filename, workfolder, output);
		converter.start();
	}



}
