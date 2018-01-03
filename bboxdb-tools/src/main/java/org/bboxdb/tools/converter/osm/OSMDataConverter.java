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
package org.bboxdb.tools.converter.osm;

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
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bboxdb.commons.concurrent.ExceptionSafeThread;
import org.bboxdb.tools.converter.osm.filter.OSMTagEntityFilter;
import org.bboxdb.tools.converter.osm.filter.multipoint.OSMBuildingsEntityFilter;
import org.bboxdb.tools.converter.osm.filter.multipoint.OSMRoadsEntityFilter;
import org.bboxdb.tools.converter.osm.filter.multipoint.OSMWaterEntityFilter;
import org.bboxdb.tools.converter.osm.filter.multipoint.WoodEntityFilter;
import org.bboxdb.tools.converter.osm.filter.singlepoint.OSMTrafficSignalEntityFilter;
import org.bboxdb.tools.converter.osm.filter.singlepoint.OSMTreeEntityFilter;
import org.bboxdb.tools.converter.osm.store.OSMBDBNodeStore;
import org.bboxdb.tools.converter.osm.store.OSMJDBCNodeStore;
import org.bboxdb.tools.converter.osm.store.OSMNodeStore;
import org.bboxdb.tools.converter.osm.store.OSMSSTableNodeStore;
import org.bboxdb.tools.converter.osm.util.Polygon;
import org.bboxdb.tools.converter.osm.util.SerializableNode;
import org.bboxdb.tools.converter.osm.util.SerializerHelper;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crosby.binary.osmosis.OsmosisReader;

public class OSMDataConverter {

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
	
	/**
	 * The amount of consumer threads
	 */
	protected final int CONSUMER_THREADS = 20;
	
	/**
	 * The Blocking queue
	 */
	protected BlockingQueue<Way> queue = new ArrayBlockingQueue<>(200);
	
	static class Backend {
		/**
		 * The name of the SSTable backend
		 */
		protected static final String SSTABLE = "sstable";
	
		/**
		 * The name of the BDB backend
		 */
		protected static final String BDB = "bdb";
	
		/**
		 * The name of the JDBC backend
		 */
		protected static final String JDBC = "jdbc";
		
		/**
		 * All known backends
		 */
		protected static final List<String> ALL_BACKENDS 
			= Arrays.asList(JDBC, BDB, SSTABLE);
	}
	
	static class Parameter {
		/**
		 * The name of the output parameter
		 */
		protected static final String OUTPUT = "output";
	
		/**
		 * The name of the workfolder
		 */
		protected static final String WORKFOLDER = "workfolder";
	
		/**
		 * The name of the backend
		 */
		protected static final String BACKEND = "backend";
	
		/**
		 * The name of the input
		 */
		protected static final String INPUT = "input";
		
		/**
		 * The name of the help parameter
		 */
		protected static final String HELP = "help";
	}
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(OSMDataConverter.class);


	static {
		filter.put(OSMType.TREE, new OSMTreeEntityFilter());
		filter.put(OSMType.WOOD, new WoodEntityFilter());
		filter.put(OSMType.TRAFFIC_SIGNAL, new OSMTrafficSignalEntityFilter());
		filter.put(OSMType.ROAD, new OSMRoadsEntityFilter());
		filter.put(OSMType.BUILDING, new OSMBuildingsEntityFilter());
		filter.put(OSMType.WATER, new OSMWaterEntityFilter());
	}
	
	public OSMDataConverter(final String filename, final String backend, 
			final String workfolder, final String output) {
		
		this.filename = filename;
		this.output = output;
		this.statistics = new OSMConverterStatistics();
		
		final File inputFile = new File(filename);

		final List<String> workfolders = Arrays.asList(workfolder.split(":"));
		 
		if(Backend.BDB.equals(backend)) {
			this.osmNodeStore = new OSMBDBNodeStore(workfolders, inputFile.length());
		} else if(Backend.JDBC.equals(backend)) {
			this.osmNodeStore = new OSMJDBCNodeStore(workfolders, inputFile.length());
		} else if(Backend.SSTABLE.equals(backend)) {
			this.osmNodeStore = new OSMSSTableNodeStore(workfolders, inputFile.length());
		} else {
			throw new RuntimeException("Unknown backend: " + backend);
		}
		
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
				public void close() {}
				
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
		
		try {
			final Options options = buildOptions();
			final CommandLineParser parser = new DefaultParser();
			final CommandLine line = parser.parse(options, args);
			
			checkParameter(options, line);
			
			final String filename = line.getOptionValue(Parameter.INPUT);
			final String backend = line.getOptionValue(Parameter.BACKEND);
			final String workfolder = line.getOptionValue(Parameter.WORKFOLDER);
			final String output = line.getOptionValue(Parameter.OUTPUT);
			
			// Check input file
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
				System.err.println("Unable to create directory: " + output);
				System.exit(-1);
			}
			
			// Check backends
			if(! Backend.ALL_BACKENDS.contains(backend)) {
				System.err.println("Backend with name is unkown: " + backend);
				printHelpAndExit(options);
			}

			final OSMDataConverter converter = new OSMDataConverter(filename, backend, workfolder, output);
			converter.start();
		} catch (ParseException e) {
			System.err.println("Unable to parse commandline arguments: " + e);
			System.exit(-1);
		}
	}

	/**
	 * Check the command line for all needed parameter
	 * @param options
	 * @param line
	 */
	protected static void checkParameter(final Options options, final CommandLine line) {
		if( line.hasOption(Parameter.HELP)) {
			printHelpAndExit(options);
		}
					
		final List<String> requiredArgs = Arrays.asList(Parameter.INPUT, 
				Parameter.OUTPUT, Parameter.BACKEND, 
				Parameter.WORKFOLDER);
		
		final boolean hasAllParameter = requiredArgs.stream().allMatch(s -> line.hasOption(s));
		
		if(! hasAllParameter) {
			printHelpAndExit(options);
		}
	}
	
	/**
	 * Build the command line options
	 * @return
	 */
	protected static Options buildOptions() {
		final Options options = new Options();
		
		// Help
		final Option help = Option.builder(Parameter.HELP)
				.desc("Show this help")
				.build();
		options.addOption(help);

		// Input file
		final Option input = Option.builder(Parameter.INPUT)
				.hasArg()
				.argName("file")
				.desc("The input file")
				.build();
		options.addOption(input);
		
		// Output dir
		final Option output = Option.builder(Parameter.OUTPUT)
				.hasArg()
				.argName("directory")
				.desc("The output directory")
				.build();
		options.addOption(output);
		
		// The backend
		final String backendList = Backend.ALL_BACKENDS
				.stream().collect(Collectors.joining(",", "[", "]"));
		
		final Option backend = Option.builder(Parameter.BACKEND)
				.hasArg()
				.argName(backendList)
				.desc("The node converter backend")
				.build();
		options.addOption(backend);

		final Option workfolder = Option.builder(Parameter.WORKFOLDER)
				.hasArg()
				.argName("workfolder1:workfolder2:workfolderN")
				.desc("The working folder for the database")
				.build();
		options.addOption(workfolder);
		return options;
	}

	/**
	 * Print help and exit the program
	 * @param options 
	 */
	protected static void printHelpAndExit(final Options options) {
		
		final String header = "OpenStreetMap data converter\n\n";
	
		final String footer = "\nPlease report issues at https://github.com/jnidzwetzki/bboxdb/issues\n";
		 
		final HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		formatter.printHelp("OSMConverter", header, options, footer);
		System.exit(-1);
	}
}