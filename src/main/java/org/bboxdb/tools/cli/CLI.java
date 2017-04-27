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
package org.bboxdb.tools.cli;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.converter.osm.OSMDataConverter;
import org.bboxdb.tools.converter.osm.util.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The BBoxDB CLI 
 *
 */
public class CLI implements Runnable, AutoCloseable {
	
	/**
	 * The parsed command line
	 */
	protected CommandLine line;
	
	/**
	 * The connection to the bboxDB Server
	 */
	protected BBoxDB bboxDbConnection;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(OSMDataConverter.class);
	
	public CLI(final CommandLine line) {
		this.line = line;
	}

	/**
	 * Main * Main * Main * Main
	 */
	public static void main(final String[] args) {
		
		// The cli instance
		CLI cli = null;
		
		try {
			final Options options = buildOptions();
			final CommandLineParser parser = new DefaultParser();
			final CommandLine line = parser.parse(options, args);
			
			checkParameter(options, line);
			
			cli = new CLI(line);
			cli.run();
		} catch (ParseException e) {
			System.err.println("Unable to parse commandline arguments: " + e);
			System.exit(-1);
		} finally {
			cli.close();
		}
	}
	
	public void run() {
		// Default values
		String zookeeperHost = "localhost:2181";
		String clustername = "mycluster";
		
		if(line.hasOption(CLIParameter.HOST)) {
			zookeeperHost = line.getOptionValue(CLIParameter.HOST);
		}
		
		if(line.hasOption(CLIParameter.CLUSTER_NAME)) {
			clustername = line.getOptionValue(CLIParameter.CLUSTER_NAME);
		}
		
		// Connect to zookeeper
		bboxDbConnection = new BBoxDBCluster(zookeeperHost, clustername);
			
		if(line.hasOption(CLIParameter.VERBOSE)) {
			org.apache.log4j.Logger logger4j = org.apache.log4j.Logger.getRootLogger();
			logger4j.setLevel(org.apache.log4j.Level.toLevel("DEBUG"));
		}
		
		final String action = line.getOptionValue(CLIParameter.ACTION);
		
		switch (action) {
		case CLIAction.CREATE_DGROUP:
			actionCreateDgroup(line);
			break;
			
		case CLIAction.DELETE_DGROUP:
			actionDeleteDgroup(line);
			break;
			
		case CLIAction.IMPORT:
			actionImportData(line);
			break;
			
		case CLIAction.QUERY:
			actionExecuteQuery(line);
			break;

		default:
			break;
		}
	}
	
	/**
	 * Shutdown the instance
	 */
	public void close() {
		if(bboxDbConnection != null) {
			bboxDbConnection.disconnect();
			bboxDbConnection = null;
		}
	}
	
	/**
	 * Execute a given query
	 * @param line
	 */
	protected void actionExecuteQuery(final CommandLine line) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * Import data
	 * @param line
	 */
	protected void actionImportData(final CommandLine line) {
		
		final List<String> requiredArgs = Arrays.asList(CLIParameter.FILE, 
				CLIParameter.FORMAT, CLIParameter.TABLE);
		
		final boolean hasAllParameter = requiredArgs.stream().allMatch(s -> line.hasOption(s));
		
		if(! hasAllParameter) {
			System.err.println("Some required parameters are not specified");
			System.exit(-1);
		}
		
		final String filename = line.getOptionValue(CLIParameter.FILE);
		final String format = line.getOptionValue(CLIParameter.FORMAT);
		final String table = line.getOptionValue(CLIParameter.TABLE);
		
		final File file = new File(filename);
		if(! file.exists()) {
			System.err.println("Unable to open file: " + file);
			System.exit(-1);
		}
		
		try(final Stream<String> fileStream = Files.lines(Paths.get(filename))) {
			long lineNumber = 1;
			
			for (Iterator<String> iterator = fileStream.iterator(); iterator.hasNext();) {
				final String fileLine = iterator.next();
				handleLine(fileLine, format, table, lineNumber);
				lineNumber++;
			}
			
		} catch (IOException e) {
			System.err.println("Got an exeption while reading file: " + e);
			System.exit(-1);
		}
		
	}

	/**
	 * Insert a line of the file into the given table
	 * 
	 * @param line
	 * @param format
	 * @param table
	 * @param lineNumber 
	 */
	protected void handleLine(final String line, final String format, final String table, 
			final long lineNumber) {
		
		if("geojson".equals(format)) {
			final Polygon polygon = Polygon.fromGeoJson(line);
	    	final byte[] tupleBytes = polygon.toGeoJson().getBytes();

			final Tuple tuple = new Tuple(Long.toString(lineNumber), 
					polygon.getBoundingBox(), tupleBytes);
			
			try {
				final EmptyResultFuture result = bboxDbConnection.insertTuple(table, tuple);
				
				result.waitForAll();
				
				if(result.isFailed()) {
					System.err.println("Got an error during insert: " + result.getAllMessages());
					System.exit(-1);
				}
				
			} catch (BBoxDBException e) {
				System.err.println("Got an error during insert: " + e);
				System.exit(-1);
			} catch (InterruptedException e) {
				System.err.println("Got an interruption during wait");
				System.exit(-1);
			}
		} else {
			throw new RuntimeException("Unknwon format: " + format);
		}
	}

	/**
	 * Delete a distribution group
	 * @param line
	 */
	protected void actionDeleteDgroup(final CommandLine line) {
		logger.debug("Delete a new distribution group");
		final String distributionGroup = line.getOptionValue(CLIParameter.DISTRIBUTION_GROUP);
		
		try {
			final EmptyResultFuture future 
				= bboxDbConnection.deleteDistributionGroup(distributionGroup);
			
			future.waitForAll();
			
			if(future.isFailed()) {
				System.err.println("Got an error during distribution group deletion: " 
						+ future.getAllMessages());
			}
		}  catch (BBoxDBException e) {
			System.err.println("Got an exception during distribution group creation: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			System.err.println("Waiting was interrupted");
			System.exit(-1);
		}
	}
	
	/**
	 * Create a new distribution group
	 * @param line
	 */
	protected void actionCreateDgroup(final CommandLine line) {
		logger.debug("Create a new distribution group");
		final String distributionGroup = line.getOptionValue(CLIParameter.DISTRIBUTION_GROUP);
		final String replicationFactorString = line.getOptionValue(CLIParameter.REPLICATION_FACTOR);
		
		try {
			final int replicationFactor = Integer.parseInt(replicationFactorString);
			final EmptyResultFuture future = bboxDbConnection.createDistributionGroup(
					distributionGroup, (short) replicationFactor);
			
			future.waitForAll();
			
			if(future.isFailed()) {
				System.err.println("Got an error during distribution group creation: " 
						+ future.getAllMessages());
			}
			
		} catch(NumberFormatException e) {
			System.err.println("This is not a valid replication factor: " + replicationFactorString);
			System.exit(-1);
		} catch (BBoxDBException e) {
			System.err.println("Got an exception during distribution group creation: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			System.err.println("Waiting was interrupted");
			System.exit(-1);
		}
	}

	/**
	 * Check the command line for all needed parameter
	 * @param options
	 * @param line
	 */
	protected static void checkParameter(final Options options, final CommandLine line) {
		
		if(line.hasOption(CLIParameter.HELP)) {
			printHelpAndExit(options);
		}
		
		if(! line.hasOption(CLIParameter.ACTION)) {
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
		final Option help = Option.builder(CLIParameter.HELP)
				.desc("Show this help")
				.build();
		options.addOption(help);
		
		// Be verbose
		final Option verbose = Option.builder(CLIParameter.VERBOSE)
				.desc("Be verbose")
				.build();
		options.addOption(verbose);
		
		// Action
		final Option action = Option.builder(CLIParameter.ACTION)
				.hasArg()
				.argName("action")
				.desc("The CLI action to execute")
				.build();
		options.addOption(action);
		
		// Host
		final Option host = Option.builder(CLIParameter.HOST)
				.hasArg()
				.argName("host")
				.desc("The Zookeeper endpoint to connect to (default: 127.0.0.1:2181)")
				.build();
		options.addOption(host);
		
		// Cluster name
		final Option clusterName = Option.builder(CLIParameter.CLUSTER_NAME)
				.hasArg()
				.argName("clustername")
				.desc("The name of the cluster (default: mycluster)")
				.build();
		options.addOption(clusterName);
		
		// Distribution group
		final Option distributionGroup = Option.builder(CLIParameter.DISTRIBUTION_GROUP)
				.hasArg()
				.argName("distributiongroup")
				.desc("The distribution group")
				.build();
		options.addOption(distributionGroup);
		
		// Replication factor
		final Option replicationFactor = Option.builder(CLIParameter.REPLICATION_FACTOR)
				.hasArg()
				.argName("replicationfactor")
				.desc("The replication factor")
				.build();
		options.addOption(replicationFactor);
		
		// Replication factor
		final Option file = Option.builder(CLIParameter.FILE)
				.hasArg()
				.argName("file")
				.desc("The file to read")
				.build();
		options.addOption(file);
		
		// Format
		final Option format = Option.builder(CLIParameter.FORMAT)
				.hasArg()
				.argName("format")
				.desc("The format of the file")
				.build();
		options.addOption(format);
		
		// Table
		final Option table = Option.builder(CLIParameter.TABLE)
				.hasArg()
				.argName("table")
				.desc("The table to carry out the action")
				.build();
		options.addOption(table);
		
		return options;
	}
	
	/**
	 * Print help and exit the program
	 * @param options 
	 */
	protected static void printHelpAndExit(final Options options) {
		final HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		formatter.printHelp("CLI", options);
		System.exit(-1);
	}

}
