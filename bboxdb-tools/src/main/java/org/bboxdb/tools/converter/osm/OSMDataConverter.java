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
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.tools.converter.osm.filter.OSMTagEntityFilter;
import org.bboxdb.tools.converter.osm.filter.multipoint.OSMBuildingsEntityFilter;
import org.bboxdb.tools.converter.osm.filter.multipoint.OSMRoadsEntityFilter;
import org.bboxdb.tools.converter.osm.filter.multipoint.OSMWaterEntityFilter;
import org.bboxdb.tools.converter.osm.filter.multipoint.WoodEntityFilter;
import org.bboxdb.tools.converter.osm.filter.singlepoint.OSMTrafficSignalEntityFilter;
import org.bboxdb.tools.converter.osm.filter.singlepoint.OSMTreeEntityFilter;
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
	
	static class Parameter {
		/**
		 * The name of the output parameter
		 */
		private static final String OUTPUT = "output";
	
		/**
		 * The name of the input
		 */
		private static final String INPUT = "input";
		
		/**
		 * The name of the help parameter
		 */
		private static final String HELP = "help";
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
	
	public OSMDataConverter(final String filename, final String output) {
		this.filename = filename;
		this.output = output;
		this.statistics = new OSMConverterStatistics();		
		statistics.start();
	}

	/**
	 * Start the converter
	 */
	public void start() {
		try {
			// Open file handles
			for(final OSMType osmType : filter.keySet()) {
				final String outputFile = output + File.separator + osmType.toString();
				final BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outputFile)));
				writerMap.put(osmType, bw);
			}
			
			System.out.format("Importing %s%n", filename);
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
					if(entityContainer.getEntity() instanceof Node) {
						final Node node = (Node) entityContainer.getEntity();
						handleNode(node);
						statistics.incProcessedNodes();
					} else if(entityContainer.getEntity() instanceof Way) {							
						final Way way = (Way) entityContainer.getEntity();
						handleWay(way);
						statistics.incProcessedWays();
					} 
				}
			});
			
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
	}

	/**
	 * Handle a node
	 * @param entityContainer
	 */
	protected void handleNode(final Node node) {
		try {			
			for(final Entry<OSMType, OSMTagEntityFilter> entry : filter.entrySet()) {
				final OSMType osmType = entry.getKey();
				final OSMTagEntityFilter entityFilter = entry.getValue();
				
				if(entityFilter.match(node.getTags())) {
					final GeoJsonPolygon geometricalStructure = new GeoJsonPolygon(node.getId());
					
					// GeoJSON: [longitude, latitude]
					geometricalStructure.addPoint(node.getLongitude(), node.getLatitude());
					
					for(final Tag tag : node.getTags()) {
						geometricalStructure.addProperty(tag.getKey(), tag.getValue());
					}
					
					writePolygonToOutput(osmType, geometricalStructure);
				}
			}
						
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
	protected void writePolygonToOutput(final OSMType osmType, final GeoJsonPolygon geometricalStructure) throws IOException {
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
			for(final Entry<OSMType, OSMTagEntityFilter> entry : filter.entrySet()) {
				final OSMType osmType = entry.getKey();
				final OSMTagEntityFilter entityFilter = entry.getValue();
				
				if(entityFilter.match(way.getTags())) {
					
					final GeoJsonPolygon geometricalStructure = new GeoJsonPolygon(way.getId());

					for(final Tag tag : way.getTags()) {
						geometricalStructure.addProperty(tag.getKey(), tag.getValue());
					}
					
 					for(final WayNode wayNode : way.getWayNodes()) { 						
 						geometricalStructure.addPoint(wayNode.getLongitude(), wayNode.getLatitude()); 						
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
			
			final OSMDataConverter converter = new OSMDataConverter(filename, output);
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
	private static void checkParameter(final Options options, final CommandLine line) {
		if( line.hasOption(Parameter.HELP)) {
			printHelpAndExit(options);
		}
					
		final List<String> requiredArgs = Arrays.asList(Parameter.INPUT, Parameter.OUTPUT);
		
		final boolean hasAllParameter = requiredArgs.stream().allMatch(s -> line.hasOption(s));
		
		if(! hasAllParameter) {
			printHelpAndExit(options);
		}
	}
	
	/**
	 * Build the command line options
	 * @return
	 */
	private static Options buildOptions() {
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
		
		return options;
	}

	/**
	 * Print help and exit the program
	 * @param options 
	 */
	private static void printHelpAndExit(final Options options) {
		
		final String header = "OpenStreetMap data converter\n\n";
	
		final String footer = "\nPlease report issues at https://github.com/jnidzwetzki/bboxdb/issues\n";
		 
		final HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		formatter.printHelp("OSMConverter", header, options, footer);
		System.exit(-1);
	}
}