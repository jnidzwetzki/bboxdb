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
import java.util.stream.Collectors;
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
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.client.tools.FixedSizeFutureStore;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
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
	 * The pending futures
	 */
	protected final FixedSizeFutureStore pendingFutures;
	
	/**
	 * The amount of pending insert futures
	 */
	protected final static int MAX_PENDING_FUTURES = 5000;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(CLI.class);
	
	public CLI(final CommandLine line) {
		this.line = line;
		
		// The pending future store
		pendingFutures = new FixedSizeFutureStore(MAX_PENDING_FUTURES);
		
		// Log failed futures
		pendingFutures.addFailedFutureCallback((f) -> logger.error("Failed future detected: {}", f));
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
		
		// Connect to zookeeper and BBoxDB
		System.out.print("Connecting to BBoxDB cluster...");
		System.out.flush();
		bboxDbConnection = new BBoxDBCluster(zookeeperHost, clustername);
		
		if( ! bboxDbConnection.connect() ) {
			System.err.println(" ERROR");
			System.err.println("Unable to connect to the BBoxDB Server");
			System.exit(-1);
		}
		
		System.out.println(" [Established]");
		
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
			
		case CLIAction.INSERT:
			actionInsertTuple(line);
			break;
			
		case CLIAction.DELETE:
			actionDeleteTuple(line);
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
		if(! line.hasOption(CLIParameter.TABLE)) {
			System.err.println("Query should be performed, but no table was specified");
			printHelpAndExit();
		}
				
		try {
			final TupleListFuture resultFuture = buildQueryFuture(line);
			
			if(resultFuture == null) {
				System.err.println("Unable to get query");
				System.exit(-1);
			}
			
			resultFuture.waitForAll();
			
			if(resultFuture.isFailed()) {
				System.err.println("Unable to execute query: " + resultFuture.getAllMessages());
				System.exit(-1);
			}
			
			for(final Tuple tuple : resultFuture) {
				System.out.format("Key %s, BoundingBox=%s, value=%s, version timestamp=%d\n",
						tuple.getKey(), tuple.getBoundingBox(), tuple.getDataBytes(), 
						tuple.getVersionTimestamp());
			}
			
			System.out.println("Query done");
		} catch (BBoxDBException e) {
			System.err.println("Got an exception while performing query: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}

	}

	/**
	 * Build the query future
	 * @param line
	 * @return
	 * @throws BBoxDBException
	 */
	protected TupleListFuture buildQueryFuture(final CommandLine line)
			throws BBoxDBException {
		
		final String table = line.getOptionValue(CLIParameter.TABLE);

		if(line.hasOption(CLIParameter.KEY)) {
			System.out.println("Executing key query..");
			final String key = line.getOptionValue(CLIParameter.KEY);
			return bboxDbConnection.queryKey(table, key);
			
		} else if(line.hasOption(CLIParameter.BOUNDING_BOX) && line.hasOption(CLIParameter.TIMESTAMP)) {
			System.out.println("Executing bounding box and time query...");
			final BoundingBox boundingBox = getBoundingBoxFromArgs(line);	
			final long timestamp = getTimestampFromArgs();
			return bboxDbConnection.queryBoundingBoxAndTime(table, boundingBox, timestamp);
			
		} else if(line.hasOption(CLIParameter.BOUNDING_BOX)) {
			System.out.println("Executing bounding box query...");
			final BoundingBox boundingBox = getBoundingBoxFromArgs(line);	
			return bboxDbConnection.queryBoundingBox(table, boundingBox);
			
		} else if(line.hasOption(CLIParameter.TIMESTAMP)) { 
			System.out.println("Executing time query...");
			final long timestamp = getTimestampFromArgs();
			return bboxDbConnection.queryVersionTime(table, timestamp);
			
		} else {
			System.err.println("Unable to execute query with the specified parameter");
			printHelpAndExit();
			
			// Unreachable code
			return null;
		}
	}

	/**
	 * Read the timestamp from CLI parameter
	 * @return
	 */
	protected long getTimestampFromArgs() {
		final String timestampString = line.getOptionValue(CLIParameter.TIMESTAMP);
		long value = -1;
		
		try {
			value = Long.parseLong(timestampString);
		} catch (NumberFormatException e) {
			System.err.println("Unable to parse timestamp: "+ timestampString);
			printHelpAndExit();
		}
	
		return value;
	}

	/**
	 * Read and convert the bounding box from CLI args
	 * @param line
	 * @return
	 */
	protected BoundingBox getBoundingBoxFromArgs(final CommandLine line) {
		final String bbox = line.getOptionValue(CLIParameter.BOUNDING_BOX);
		
		final String[] bboxStringParts = bbox.split(":");
		
		if(bboxStringParts.length % 2 != 0) {
			System.err.println("Invalid bounding box: " + bbox);
			System.exit(-1);
		}
		
		final double[] bboxDoubleValues = new double[bboxStringParts.length];
		for(int i = 0; i < bboxStringParts.length; i++) {
			try {
				bboxDoubleValues[i] = Double.parseDouble(bboxStringParts[i]);
			} catch (NumberFormatException e) {
				System.err.println("Invalid number: " + bboxStringParts[i]);
			}
		}
		
		return new BoundingBox(bboxDoubleValues);
	}
	
	/**
	 * Delete a tuple
	 * @param line
	 */
	protected void actionDeleteTuple(final CommandLine line) {
		
		if(! line.hasOption(CLIParameter.KEY) || ! line.hasOption(CLIParameter.TABLE)) {
			System.err.println("Key or table are missing");
			printHelpAndExit();
		}
		
		final String key = line.getOptionValue(CLIParameter.KEY);
		final String table = line.getOptionValue(CLIParameter.TABLE);
		
		try {
			final EmptyResultFuture resultFuture = bboxDbConnection.deleteTuple(table, key);
			pendingFutures.put(resultFuture);
			pendingFutures.waitForCompletion();
		} catch (BBoxDBException e) {
			System.err.println("Got an error during delete: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}

	/**
	 * Check the required args
	 * @param requiredArgs
	 */
	protected void checkRequiredArgs(final List<String> requiredArgs) {
		for(final String arg : requiredArgs) {
			if(! line.hasOption(arg)) {
				System.err.println("Option is missing: " + arg);
				printHelpAndExit();
			}
		}
	}
	
	/**
	 * Insert a new tuple
	 * @param line
	 */
	protected void actionInsertTuple(final CommandLine line) {
		final List<String> requiredArgs = Arrays.asList(CLIParameter.TABLE,
				CLIParameter.KEY, CLIParameter.BOUNDING_BOX, CLIParameter.VALUE);
		
		checkRequiredArgs(requiredArgs);
		
		final String table = line.getOptionValue(CLIParameter.TABLE);
		final String key = line.getOptionValue(CLIParameter.KEY);
		final String value = line.getOptionValue(CLIParameter.VALUE);
		final BoundingBox boundingBox = getBoundingBoxFromArgs(line);
		
		final Tuple tuple = new Tuple(key, boundingBox, value.getBytes());
		
		System.out.println("Insert new tuple into table: " + table);
		
		try {
			final EmptyResultFuture future = bboxDbConnection.insertTuple(table, tuple);
			pendingFutures.put(future);
			pendingFutures.waitForCompletion();
		} catch (BBoxDBException e) {
			System.err.println("Got an error during insert: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		} 
	}

	/**
	 * Import data
	 * @param line
	 */
	protected void actionImportData(final CommandLine line) {
		
		final List<String> requiredArgs = Arrays.asList(CLIParameter.FILE, 
				CLIParameter.FORMAT, CLIParameter.TABLE);
		
		checkRequiredArgs(requiredArgs);
		
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
			
			pendingFutures.waitForCompletion();
			System.out.format("Successfully imported %d lines\n", lineNumber);
			
		} catch (IOException e) {
			System.err.println("Got an exeption while reading file: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
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
				pendingFutures.put(result);
				pendingFutures.waitForCompletion();
			} catch (BBoxDBException e) {
				System.err.println("Got an error during insert: " + e);
				System.exit(-1);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
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
		final String distributionGroup = line.getOptionValue(CLIParameter.DISTRIBUTION_GROUP);
		
		System.out.println("Deleting distribution group: " + distributionGroup);

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
		
		final List<String> requiredArgs = Arrays.asList(CLIParameter.DISTRIBUTION_GROUP, 
				CLIParameter.REPLICATION_FACTOR);
			
		checkRequiredArgs(requiredArgs);
		
		final String distributionGroup = line.getOptionValue(CLIParameter.DISTRIBUTION_GROUP);
		final String replicationFactorString = line.getOptionValue(CLIParameter.REPLICATION_FACTOR);
		
		System.out.println("Create new distribution group: " + distributionGroup);
		
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
			printHelpAndExit();
		}
		
		if(! line.hasOption(CLIParameter.ACTION)) {
			printHelpAndExit();
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
		final String allActions = CLIAction.ALL_ACTIONS
				.stream().collect(Collectors.joining(",", "[", "]"));
		
		final Option action = Option.builder(CLIParameter.ACTION)
				.hasArg()
				.argName(allActions)
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
		
		// Key
		final Option key = Option.builder(CLIParameter.KEY)
				.hasArg()
				.argName("key")
				.desc("The name of the key")
				.build();
		options.addOption(key);
		
		// BBox
		final Option bbox = Option.builder(CLIParameter.BOUNDING_BOX)
				.hasArg()
				.argName("bounding box")
				.desc("The bounding box of the tuple")
				.build();
		options.addOption(bbox);
		
		// Value
		final Option value = Option.builder(CLIParameter.VALUE)
				.hasArg()
				.argName("value")
				.desc("The value of the tuple")
				.build();
		options.addOption(value);
		
		// Time
		final Option time = Option.builder(CLIParameter.TIMESTAMP)
				.hasArg()
				.argName("timestamp")
				.desc("The version time stamp of the tuple")
				.build();
		options.addOption(time);
		
		return options;
	}
	
	/**
	 * Print help and exit the program
	 * @param options 
	 */
	protected static void printHelpAndExit() {
		
		final Options options = buildOptions();
		
		final String header = "BBoxDB command line interace (CLI)\n\n";
		final String footer = "\nPlease report issues at https://github.com/jnidzwetzki/bboxdb/issues\n";
		 
		final HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(120);
		formatter.printHelp("CLI", header, options, footer);
		
		System.exit(-1);
	}

}
